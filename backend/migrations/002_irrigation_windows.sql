-- Irrigation rounds now record the actual started_at/ended_at clock window.
-- Sample shifts that overlap that window stay on file but are excluded from the
-- cluster snapshot (recorded as missing instead).
ALTER TABLE irrigation_round ADD COLUMN IF NOT EXISTS started_at timestamptz;
ALTER TABLE irrigation_round ADD COLUMN IF NOT EXISTS ended_at timestamptz;

UPDATE irrigation_round
SET started_at = coalesce(started_at, starts_at),
    ended_at = coalesce(ended_at, completed_at)
WHERE state = 'completed' AND (started_at IS NULL OR ended_at IS NULL);

ALTER TABLE irrigation_round DROP CONSTRAINT IF EXISTS round_actual_window;
ALTER TABLE irrigation_round ADD CONSTRAINT round_actual_window CHECK (
    (started_at IS NULL) = (ended_at IS NULL)
    AND (started_at IS NULL OR ended_at > started_at)
);

CREATE TABLE IF NOT EXISTS sample_exclusion (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_id integer NOT NULL REFERENCES microclimate_sample(id) ON DELETE CASCADE,
    round_id integer REFERENCES irrigation_round(id),
    block_id integer NOT NULL REFERENCES vine_block(id),
    observed_day date NOT NULL,
    slot text NOT NULL CHECK (slot IN ('day', 'night')),
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    reason text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sample_id, round_id)
);
CREATE INDEX IF NOT EXISTS sample_exclusion_round ON sample_exclusion (round_id);
CREATE INDEX IF NOT EXISTS sample_exclusion_sample ON sample_exclusion (sample_id);

-- Backfill exclusions for rounds completed before the window columns existed,
-- using the best available window (scheduled start to recorded completion).
INSERT INTO sample_exclusion(sample_id,round_id,block_id,observed_day,slot,started_at,ended_at,reason)
SELECT s.id, r.id, s.block_id, s.observed_day, s.slot, r.started_at, r.ended_at,
       '与轮次 ' || r.code || ' 实际灌溉时段重叠'
FROM irrigation_round r
JOIN microclimate_sample s ON s.block_id = r.block_id
WHERE r.state = 'completed' AND r.started_at IS NOT NULL AND r.ended_at IS NOT NULL
  AND s.observed_day BETWEEN (r.started_at AT TIME ZONE 'Asia/Shanghai')::date - 1
                         AND (r.ended_at AT TIME ZONE 'Asia/Shanghai')::date + 1
  AND r.started_at < (s.observed_day
        + CASE s.slot WHEN 'day' THEN interval '18 hours' ELSE interval '30 hours' END)
        AT TIME ZONE 'Asia/Shanghai'
  AND r.ended_at > (s.observed_day
        + CASE s.slot WHEN 'day' THEN interval '6 hours' ELSE interval '18 hours' END)
        AT TIME ZONE 'Asia/Shanghai'
ON CONFLICT (sample_id,round_id) DO NOTHING;

ALTER TABLE climate_run DROP CONSTRAINT IF EXISTS climate_run_status_check;
ALTER TABLE climate_run ADD CONSTRAINT climate_run_status_check
    CHECK (status IN ('blocked', 'queued', 'effective', 'review', 'rejected', 'failed', 'superseded'));

ALTER TABLE climate_run ADD COLUMN IF NOT EXISTS superseded_by integer REFERENCES climate_run(id);
ALTER TABLE climate_run ADD COLUMN IF NOT EXISTS superseded_reason text;

