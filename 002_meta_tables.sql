\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f 002_meta_tables.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 002_meta_tables.sql

\if :{?db_name}
\connect :db_name
\endif

CREATE SCHEMA IF NOT EXISTS meta;

-- Keep updated_at current on mutable registry tables.
CREATE OR REPLACE FUNCTION meta.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS meta.source_registry (
  source_id BIGSERIAL PRIMARY KEY,
  source_code TEXT NOT NULL UNIQUE,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  provider TEXT,
  license TEXT,
  citation TEXT,
  source_url TEXT,
  acquisition_method TEXT,
  acquisition_date DATE,
  temporal_start DATE,
  temporal_end DATE,
  declared_crs TEXT,
  refresh_cadence TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT source_registry_temporal_order_chk
    CHECK (temporal_end IS NULL OR temporal_start IS NULL OR temporal_end >= temporal_start)
);

CREATE TABLE IF NOT EXISTS meta.layer_registry (
  layer_id BIGSERIAL PRIMARY KEY,
  layer_key TEXT NOT NULL UNIQUE,
  layer_name TEXT NOT NULL,
  layer_family TEXT NOT NULL,
  data_kind TEXT NOT NULL,
  source_id BIGINT REFERENCES meta.source_registry(source_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  source_layer_name TEXT,
  source_path TEXT,
  canonical_schema TEXT NOT NULL,
  canonical_table TEXT NOT NULL,
  file_format TEXT,
  geometry_type TEXT,
  declared_crs TEXT,
  city TEXT,
  readiness_status TEXT NOT NULL DEFAULT 'discovered',
  is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
  row_count BIGINT,
  last_refresh_at TIMESTAMPTZ,
  validation_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT layer_registry_data_kind_chk
    CHECK (data_kind IN ('vector', 'raster', 'table', 'composite')),
  CONSTRAINT layer_registry_row_count_chk
    CHECK (row_count IS NULL OR row_count >= 0)
);

CREATE TABLE IF NOT EXISTS meta.metric_registry (
  metric_id TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  category TEXT NOT NULL,
  display_default BOOLEAN NOT NULL DEFAULT FALSE,
  unit TEXT,
  formula_summary TEXT NOT NULL,
  source_layers TEXT[] NOT NULL DEFAULT '{}'::text[],
  feasibility TEXT NOT NULL,
  release_target TEXT NOT NULL,
  status TEXT NOT NULL,
  null_rule TEXT NOT NULL,
  higher_is_better BOOLEAN,
  interpretation_direction TEXT,
  frontend_group TEXT NOT NULL,
  validation_rule TEXT NOT NULL,
  backend_function TEXT,
  api_field TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT metric_registry_status_chk
    CHECK (
      status IN (
        'implemented',
        'planned',
        'blocked_data',
        'proxy_only',
        'deprecated_or_revised'
      )
    ),
  CONSTRAINT metric_registry_feasibility_chk
    CHECK (feasibility IN ('High', 'Medium', 'Low-Medium', 'Low')),
  CONSTRAINT metric_registry_release_target_final_chk
    CHECK (release_target = 'final'),
  CONSTRAINT metric_registry_direction_chk
    CHECK (higher_is_better IS NOT NULL OR interpretation_direction IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
  run_id BIGSERIAL PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  run_type TEXT NOT NULL DEFAULT 'manual',
  run_status TEXT NOT NULL DEFAULT 'queued',
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  initiated_by TEXT,
  city_scope TEXT[] NOT NULL DEFAULT '{}'::text[],
  input_layers TEXT[] NOT NULL DEFAULT '{}'::text[],
  output_layers TEXT[] NOT NULL DEFAULT '{}'::text[],
  run_params JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
  warning_count INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  error_log TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pipeline_runs_run_type_chk
    CHECK (run_type IN ('manual', 'scheduled', 'backfill', 'adhoc', 'test')),
  CONSTRAINT pipeline_runs_run_status_chk
    CHECK (run_status IN ('queued', 'running', 'success', 'failed', 'partial_success', 'cancelled')),
  CONSTRAINT pipeline_runs_finish_after_start_chk
    CHECK (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at),
  CONSTRAINT pipeline_runs_counts_chk
    CHECK (warning_count >= 0 AND error_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_source_registry_active
  ON meta.source_registry (is_active);

CREATE INDEX IF NOT EXISTS idx_layer_registry_family_city
  ON meta.layer_registry (layer_family, city);

CREATE INDEX IF NOT EXISTS idx_layer_registry_readiness
  ON meta.layer_registry (readiness_status);

CREATE INDEX IF NOT EXISTS idx_metric_registry_status_release
  ON meta.metric_registry (status, release_target);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_status_started
  ON meta.pipeline_runs (pipeline_name, run_status, started_at DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_source_registry_touch_updated_at'
  ) THEN
    CREATE TRIGGER trg_source_registry_touch_updated_at
    BEFORE UPDATE ON meta.source_registry
    FOR EACH ROW
    EXECUTE FUNCTION meta.touch_updated_at();
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_layer_registry_touch_updated_at'
  ) THEN
    CREATE TRIGGER trg_layer_registry_touch_updated_at
    BEFORE UPDATE ON meta.layer_registry
    FOR EACH ROW
    EXECUTE FUNCTION meta.touch_updated_at();
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_metric_registry_touch_updated_at'
  ) THEN
    CREATE TRIGGER trg_metric_registry_touch_updated_at
    BEFORE UPDATE ON meta.metric_registry
    FOR EACH ROW
    EXECUTE FUNCTION meta.touch_updated_at();
  END IF;
END
$$;
