UPDATE mining_window_fixed
SET status = 'LTS',
    snapshot = jsonb_set(snapshot, '{status}', '"LTS"'),
    updated_at = now()
WHERE status <> 'LTS'
  AND window_end < now() - interval '12 minutes';