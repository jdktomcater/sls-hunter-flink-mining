-- One fixed event-time window per index_family + window_start.
-- The window job runs this statement on startup. Apply it here when the job user cannot create tables.
-- A replayed or older revision does not overwrite a newer row.
-- status is text: FINAL / CORRECTED are mutable, LTS means unchanged for 12 minutes and may be corrected later.
CREATE TABLE IF NOT EXISTS mining_window_fixed (
  index_family text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  target_day date NOT NULL,
  revision bigint NOT NULL,
  status text NOT NULL,
  schema_version text NOT NULL,
  rule_version text NOT NULL,
  computed_at timestamptz NOT NULL,
  signal_count integer NOT NULL,
  snapshot jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (index_family, window_start)
);
