import logging
import time
from pathlib import Path

from .clusters import store_fit
from .db import EVIDENCE_DIR, QUEUE, connect, event, lock_ledger, redis_client
from .mixture import fit_profiles

logger = logging.getLogger('vineyard.worker')


def consume_one(queued_ids=()):
    with connect() as db:
        lock_ledger(db)
        # Prefer batches whose notification arrived (ordered queue), then any
        # queued batch so a lost Redis message still recovers on the next loop.
        run = db.execute('''
            SELECT c.id,c.snapshot,c.digest FROM climate_run c
            WHERE c.status='queued'
            ORDER BY (c.id::text = ANY(%s)) DESC, c.id LIMIT 1
        ''', (list(queued_ids),)).fetchone()
        if not run:
            return
        run_id = run['id']
        # A late completion or a supervisor decision may have voided this batch
        # between dequeue and processing; re-check under the ledger lock.
        live = db.execute('SELECT status FROM climate_run WHERE id=%s', (run_id,)).fetchone()
        if live['status'] != 'queued':
            logger.info('Skipping climate run %s now %s', run_id, live['status'])
            return
        try:
            result = fit_profiles(run['snapshot'])
        except Exception as exc:
            logger.exception('Fit failed for run %s', run_id)
            reason = str(exc) if isinstance(exc, ValueError) else '混合模型计算失败，可按原快照重试。'
            if db.execute("SELECT 1 FROM climate_run WHERE id=%s AND status='queued'", (run_id,)).fetchone():
                db.execute("UPDATE climate_run SET status='failed',error=%s,attempts=attempts+1 WHERE id=%s",
                           (reason, run_id))
                event(db, 'worker', 'fit_failed', run_id, digest=run['digest'], reason=reason)
            return
        if not db.execute("SELECT 1 FROM climate_run WHERE id=%s AND status='queued'", (run_id,)).fetchone():
            logger.info('Climate run %s was superseded during fit; result discarded', run_id)
            return
        store_fit(db, run_id, result)
    # PostgreSQL remains authoritative if archiving fails after the transaction commits.
    try:
        with connect() as db:
            row = db.execute('''
                SELECT result, jsonb_build_object('id',id,'window_end',window_end,'digest',digest,
                    'snapshot',snapshot,'result',result)::text AS payload
                FROM climate_run WHERE id=%s
            ''', (run_id,)).fetchone()
        if row['result'] != result:
            logger.info('Climate run %s was voided before archiving; kept %s',
                        run_id, 'prior evidence' if row['result'] else 'no evidence')
            return
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = EVIDENCE_DIR / f'run-{run_id}.tmp'
        tmp.write_text(row['payload'], encoding='utf-8')
        tmp.replace(EVIDENCE_DIR / f'run-{run_id}.json')
    except Exception:
        logger.exception('Could not archive run %s', run_id)
    logger.info('Committed climate run %s (%s components)', run_id, result['chosen_k'])


def main():
    logging.basicConfig(level=logging.INFO)
    client = redis_client()
    heartbeat = Path('/tmp/field-heartbeat')
    while True:
        try:
            notice = client.blpop(QUEUE, timeout=2)
            queued_ids = [notice[1].decode()] if notice and notice[1] != b'wake' else []
            # The persisted status recovers work even if a notification or worker is lost.
            consume_one(queued_ids)
            heartbeat.touch()
        except Exception:
            logger.exception('Worker iteration failed')
            time.sleep(2)


if __name__ == '__main__':
    main()
