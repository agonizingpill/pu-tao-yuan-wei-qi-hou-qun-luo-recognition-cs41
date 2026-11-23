import hashlib
import json
from datetime import timedelta

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .db import event
from .domain import RunState, Slot
from .exclusions import window_exclusions
from .fieldbook import one

RUN_FIELDS = ('id,window_end,status,digest,coverage,result,error,attempts,actor,created_at,'
              'superseded_by,superseded_reason')
WINDOW_DAYS = 3


def _exclusion_index(db, start, end):
    index = {}
    for row in window_exclusions(db, start, end):
        key = (row['block_id'], row['observed_day'], row['slot'])
        index.setdefault(key, []).append(row)
    return index


def make_snapshot(db, end):
    start = end - timedelta(days=WINDOW_DAYS - 1)
    blocks = db.execute('SELECT id,code,sensor FROM vine_block ORDER BY code').fetchall()
    samples = db.execute('''
        SELECT id,block_id,observed_day,slot,temp_c,rh_pct,source
        FROM microclimate_sample WHERE observed_day BETWEEN %s AND %s ORDER BY observed_day,slot,id
    ''', (start, end)).fetchall()
    exclusions = _exclusion_index(db, start, end)
    snapshot, coverage = [], []
    expected = {(start + timedelta(days=i), slot.value) for i in range(WINDOW_DAYS) for slot in Slot}
    for block in blocks:
        readings = [row for row in samples if row['block_id'] == block['id']]
        admitted = [row for row in readings
                    if (block['id'], row['observed_day'], row['slot']) not in exclusions]
        excluded = []
        for key, rows in exclusions.items():
            if key[0] != block['id']:
                continue
            reading = next((row for row in rows if row['sample_id'] is not None), None)
            excluded.append({'sample_id': reading['sample_id'] if reading else None,
                             'day': key[1].isoformat(), 'slot': key[2],
                             'temp_c': reading['temp_c'] if reading else None,
                             'rh_pct': reading['rh_pct'] if reading else None,
                             'source': reading['source'] if reading else None,
                             'rounds': sorted({row['round_code'] for row in rows if row['round_code']})})
        excluded.sort(key=lambda item: (item['day'], item['slot']))
        missing = sorted(expected - {(row['observed_day'], row['slot']) for row in admitted})
        snapshot.append(dict(
            block_id=block['id'], code=block['code'], sensor=block['sensor'],
            readings=[dict(row, observed_day=row['observed_day'].isoformat()) for row in admitted],
            excluded=excluded))
        coverage.append(dict(code=block['code'], block_id=block['id'], sensor=block['sensor'],
                             received=len(admitted), expected=len(expected),
                             missing=[{'day': day.isoformat(), 'slot': slot} for day, slot in missing],
                             excluded=excluded, deficient=len(missing) >= 2))
    return snapshot, coverage


def _digest(end, snapshot):
    return hashlib.sha256(json.dumps({'window_end': end.isoformat(), 'blocks': snapshot},
                                     ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _assess(db, end):
    snapshot, coverage = make_snapshot(db, end)
    digest = _digest(end, snapshot)
    prior = db.execute('''
        SELECT id FROM climate_run WHERE digest=%s AND status='effective' ORDER BY id DESC LIMIT 1
    ''', (digest,)).fetchone()
    reason = None
    if len(snapshot) < 6:
        reason = '至少需要 6 个地块才能识别群落。'
    deficient = [entry['code'] for entry in coverage if entry['deficient']]
    if deficient:
        reason = '缺测超过 25%，请补录后再次提交：' + '、'.join(deficient)
    return snapshot, coverage, digest, prior, reason, deficient


def _open_run(db):
    return db.execute("SELECT id FROM climate_run WHERE status IN ('queued','review') LIMIT 1").fetchone()


def _new_run(db, end, snapshot, coverage, digest, reason, actor):
    state = RunState.BLOCKED if reason else RunState.QUEUED
    return db.execute('''
        INSERT INTO climate_run(window_end,status,snapshot,digest,coverage,error,actor)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    ''', (end, state, Jsonb(snapshot), digest, Jsonb(coverage), reason, actor)).fetchone()['id']


def submit(db, end, actor, trigger=None):
    if _open_run(db):
        raise HTTPException(409, '请先处理当前识别批次')
    latest = db.execute("SELECT max(window_end) AS day FROM climate_run WHERE status='effective'").fetchone()['day']
    if latest is not None and end < latest:
        raise HTTPException(409, '窗口早于当前生效标签的采样窗口')
    snapshot, coverage, digest, prior, reason, deficient = _assess(db, end)
    if prior:
        return run_detail(db, prior['id'])
    run_id = _new_run(db, end, snapshot, coverage, digest, reason, actor)
    event(db, actor, 'cluster_blocked' if reason else 'snapshot_queued', run_id,
          digest=digest, reason=reason, deficient=deficient, trigger=trigger)
    return run_detail(db, run_id)


def _covers(block_entry, sample):
    """Whether a pending run's frozen snapshot actually took this reading."""
    return (block_entry['block_id'] == sample['block_id']
            and any(reading['id'] == sample['id'] for reading in block_entry['readings']))


def void_and_recompute(db, newly, actor, trigger_code):
    """Void pending batches a late completion invalidates, then recompute the window.

    A completion that lands after a batch froze its snapshot but before that batch
    is posted or signed invalidates the batch. The replacement window is computed
    from the same cut-off date with the overlapping shifts excluded. A batch that
    has already been published stays in force.
    """
    if not newly:
        return None, [], []
    pending = db.execute('''
        SELECT id,window_end,status,created_at,snapshot FROM climate_run
        WHERE status IN ('queued','review') ORDER BY id
    ''').fetchall()
    voided, replacement, requeued = [], None, []
    for run in pending:
        entries = {item['block_id']: item for item in run['snapshot']}
        affected = [(sample, codes) for sample, codes in newly
                    if sample['block_id'] in entries
                    and _covers(entries[sample['block_id']], sample)]
        if not affected:
            continue
        db.execute("UPDATE climate_label SET state='rejected' WHERE run_id=%s AND state='proposed'", (run['id'],))
        db.execute('''
            UPDATE climate_run SET status='superseded', superseded_reason=%s WHERE id=%s
        ''', (f'晚到完灌 {trigger_code} 与批次快照重叠', run['id']))
        voided.append(run['id'])
        snapshot, coverage, digest, prior, reason, deficient = _assess(db, run['window_end'])
        target = prior['id'] if prior else _new_run(db, run['window_end'], snapshot, coverage, digest,
                                                   reason, actor)
        db.execute('UPDATE climate_run SET superseded_by=%s WHERE id=%s', (target, run['id']))
        replacement = target
        if prior is None:
            requeued.append(target)
            event(db, actor, 'cluster_blocked' if reason else 'snapshot_requeued', target,
                  digest=digest, reason=reason, deficient=deficient,
                  superseded=run['id'], trigger=trigger_code)
        event(db, actor, 'snapshot_superseded', run['id'],
              superseded_by=target, trigger=trigger_code,
              shifts=[{'block': sample['block_id'], 'day': sample['observed_day'].isoformat(),
                       'slot': sample['slot'], 'rounds': codes} for sample, codes in affected])
    return replacement, voided, requeued


def affected_rounds(db, run_id):
    return db.execute('''
        SELECT r.id,r.code,b.code AS block,p.profile AS proposed_profile,a.profile AS basis_profile,r.starts_at
        FROM irrigation_round r JOIN vine_block b ON b.id=r.block_id
        JOIN climate_label p ON p.block_id=r.block_id AND p.run_id=%s AND p.state='proposed'
        JOIN climate_label a ON a.id=r.label_id
        WHERE r.state='scheduled' AND a.profile<>p.profile
        ORDER BY r.starts_at,r.code
    ''', (run_id,)).fetchall()


def publish(db, run_id, actor, reason, affected):
    db.execute('''
        UPDATE climate_label current SET state='superseded'
        FROM climate_label proposed
        WHERE current.state='active' AND proposed.run_id=%s AND proposed.state='proposed'
          AND proposed.block_id=current.block_id
    ''', (run_id,))
    db.execute("UPDATE climate_label SET state='active' WHERE run_id=%s AND state='proposed'", (run_id,))
    if affected:
        db.execute("UPDATE irrigation_round SET state='needs_replan' WHERE id=ANY(%s)",
                   ([item['id'] for item in affected],))
    db.execute("UPDATE climate_run SET status='effective' WHERE id=%s", (run_id,))
    event(db, actor, 'labels_published', run_id, reason=reason, impact=[item['code'] for item in affected])


def store_fit(db, run_id, result):
    # A late completion may have voided this batch while the fit was running.
    if not db.execute("SELECT 1 FROM climate_run WHERE id=%s AND status='queued'", (run_id,)).fetchone():
        return
    for item in result['assignments']:
        block_id = item['block_id']
        db.execute('''
            INSERT INTO climate_label(block_id,run_id,revision,profile,confidence,state)
            SELECT %s,%s,COALESCE(max(revision),0)+1,%s,%s,'proposed'
            FROM climate_label WHERE block_id=%s
        ''', (block_id, run_id, item['profile'], item['confidence'], block_id))
    impacted = affected_rounds(db, run_id)
    db.execute('UPDATE climate_run SET status=%s,result=%s,error=NULL,attempts=attempts+1 WHERE id=%s',
               ('review' if impacted else 'queued', Jsonb(result), run_id))
    if impacted:
        event(db, 'worker', 'review_required', run_id, rounds=[item['code'] for item in impacted])
    else:
        publish(db, run_id, 'worker', '没有已排轮次受到影响', [])


def decide(db, run_id, approve, reason, actor):
    run = run_detail(db, run_id)
    if run['status'] == RunState.SUPERSEDED:
        raise HTTPException(409, '该批次已被晚到的完灌记录作废，请处理新的识别批次')
    if run['status'] != RunState.REVIEW:
        raise HTTPException(409, '该识别批次不在主管确认阶段')
    if approve:
        publish(db, run_id, actor, reason, run['affected'])
    else:
        db.execute("UPDATE climate_label SET state='rejected' WHERE run_id=%s AND state='proposed'", (run_id,))
        db.execute("UPDATE climate_run SET status='rejected' WHERE id=%s", (run_id,))
        event(db, actor, 'labels_rejected', run_id, reason=reason)
    return run_detail(db, run_id)


def retry(db, run_id, actor):
    run = run_detail(db, run_id)
    newer = db.execute('SELECT 1 FROM climate_run WHERE id>%s LIMIT 1', (run_id,)).fetchone()
    if run['status'] != RunState.FAILED or newer:
        raise HTTPException(409, '只有最新一笔失败的计算可以按原快照重试')
    db.execute("UPDATE climate_run SET status='queued',error=NULL WHERE id=%s", (run_id,))
    event(db, actor, 'snapshot_retried', run_id, digest=run['digest'])
    return run_detail(db, run_id)


def run_detail(db, run_id):
    run = one(db, f'SELECT {RUN_FIELDS} FROM climate_run WHERE id=%s', (run_id,), '识别批次')
    run['affected'] = affected_rounds(db, run_id) if run['status'] == RunState.REVIEW else []
    # Frozen exclusions describe what this batch's model input omitted. Live
    # exclusions in the same window may also cover shifts recorded after the
    # snapshot; surface both so the trace explains later voiding as well.
    frozen = {}
    for entry in run['coverage']:
        for item in entry.get('excluded', []):
            frozen[(entry['block_id'], item['day'], item['slot'])] = item
    live = dict(frozen)
    codes = {entry['block_id']: entry['code'] for entry in run['coverage']}
    window_end = run['window_end']
    for row in window_exclusions(db, window_end - timedelta(days=WINDOW_DAYS - 1), window_end):
        if row['block_id'] not in codes:
            continue
        key = (row['block_id'], row['observed_day'].isoformat(), row['slot'])
        base = live.get(key, {'rounds': [], 'temp_c': row['temp_c'],
                              'rh_pct': row['rh_pct'], 'source': row['source']})
        rounds = set(base['rounds'])
        if row['round_code']:
            rounds.add(row['round_code'])
        live[key] = dict(base, rounds=sorted(rounds))
    shifts = [{'block': codes[key[0]], 'day': key[1], 'slot': key[2],
               'rounds': item['rounds'], 'temp_c': item.get('temp_c'),
               'rh_pct': item.get('rh_pct'), 'source': item.get('source'),
               'applied': key in frozen}
              for key, item in live.items()]
    shifts.sort(key=lambda item: (item['block'], item['day'], item['slot']))
    run['excluded_shifts'] = shifts
    if run.get('superseded_by'):
        run['superseded_by_code'] = db.execute(
            'SELECT id,window_end,status FROM climate_run WHERE id=%s',
            (run['superseded_by'],)).fetchone()
    return run
