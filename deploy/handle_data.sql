-- Optional one-time PostgreSQL backfill for rows produced before processing-time LTS timers.
-- Run only after the upgraded job is running. This updates PostgreSQL only, not Kafka or Flink state.
-- A later, higher-revision snapshot can set CORRECTED and start a new stability period.
UPDATE mining_window_fixed
SET status = 'LTS',
    snapshot = jsonb_set(snapshot, '{status}', '"LTS"'),
    updated_at = now()
WHERE status IN ('FINAL', 'CORRECTED')
  AND updated_at <= now() - interval '12 minutes';
