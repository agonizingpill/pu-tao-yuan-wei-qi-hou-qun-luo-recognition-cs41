"""Reconcile shift readings against the actual windows of completed irrigation.

Readings are never deleted: a shift that overlaps a completed round's real
start/stop clock window keeps its row in `microclimate_sample` and gains a
`sample_exclusion` row that marks it as missing for model input. Several rounds
may overlap the same shift; each overlap is recorded separately so the
identification page can trace which watering excluded which shift.
"""

from datetime import timedelta

from .windows import CST, shift_bounds

SAMPLE_COLUMNS = 'id,block_id,observed_day,slot'


def _sample_start_end(sample):
    return shift_bounds(sample['observed_day'], sample['slot'])


def overlapping_rounds(db, sample):
    start, end = _sample_start_end(sample)
    return db.execute('''
        SELECT id,code,started_at,ended_at FROM irrigation_round
        WHERE block_id=%s AND state='completed' AND started_at IS NOT NULL AND ended_at IS NOT NULL
          AND started_at < %s AND ended_at > %s
        ORDER BY started_at,id
    ''', (sample['block_id'], end, start)).fetchall()


def refresh_samples(db, samples):
    """Bring sample_exclusion in line with completed rounds for these samples.

    Returns the samples that had no exclusion before this call but are excluded
    now, paired with the round codes responsible. Those are the only ones that
    can newly change a frozen model snapshot.
    """
    newly = []
    for sample in samples:
        rounds = overlapping_rounds(db, sample)
        wanted = {row['id'] for row in rounds}
        have = {row['round_id'] for row in db.execute(
            'SELECT round_id FROM sample_exclusion WHERE sample_id=%s', (sample['id'],)).fetchall()}
        for row in rounds:
            if row['id'] in have:
                continue
            db.execute('''
                INSERT INTO sample_exclusion(sample_id,round_id,block_id,observed_day,slot,started_at,ended_at,reason)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sample_id,round_id) DO NOTHING
            ''', (sample['id'], row['id'], sample['block_id'], sample['observed_day'], sample['slot'],
                  row['started_at'], row['ended_at'], f"与轮次 {row['code']} 实际灌溉时段重叠"))
        stale = have - wanted
        if stale:
            db.execute('DELETE FROM sample_exclusion WHERE sample_id=%s AND round_id=ANY(%s)',
                       (sample['id'], list(stale)))
        if wanted and not have:
            newly.append((sample, sorted(row['code'] for row in rounds)))
    return newly


def samples_for_round(db, round_row):
    """All recorded shifts that could fall inside this round's actual window."""
    start = round_row['started_at'].astimezone(CST).date() - timedelta(days=1)
    end = round_row['ended_at'].astimezone(CST).date() + timedelta(days=1)
    return db.execute(f'''
        SELECT {SAMPLE_COLUMNS} FROM microclimate_sample
        WHERE block_id=%s AND observed_day BETWEEN %s AND %s
    ''', (round_row['block_id'], start, end)).fetchall()


def window_exclusions(db, window_start, window_end):
    """Exclusion rows for shifts observed inside a three-day sampling window."""
    return db.execute('''
        SELECT e.sample_id,e.block_id,e.observed_day,e.slot,e.started_at,e.ended_at,
               r.code AS round_code,s.temp_c,s.rh_pct,s.source
        FROM sample_exclusion e
        LEFT JOIN irrigation_round r ON r.id=e.round_id
        LEFT JOIN microclimate_sample s ON s.id=e.sample_id
        WHERE e.observed_day BETWEEN %s AND %s
        ORDER BY e.observed_day,e.slot,r.code
    ''', (window_start, window_end)).fetchall()
