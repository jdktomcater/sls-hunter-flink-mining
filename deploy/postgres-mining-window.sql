-- Current mining window. One row per index_family + target_day.
-- The job runs this statement on startup. Apply it here when the job user cannot create tables.
-- A replayed or older revision does not overwrite a newer row.
CREATE TABLE IF NOT EXISTS mining_window (
  index_family text NOT NULL,
  target_day date NOT NULL,
  revision bigint NOT NULL,
  status text NOT NULL,
  schema_version text NOT NULL,
  rule_version text NOT NULL,
  computed_at timestamptz NOT NULL,
  signal_count integer NOT NULL,
  snapshot jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (index_family, target_day)
);
