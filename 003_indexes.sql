\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f 003_indexes.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 003_indexes.sql

\if :{?db_name}
\connect :db_name
\endif

-- ---------------------------------------------------------------------------
-- Meta table hardening: mandatory metadata constraints
-- ---------------------------------------------------------------------------

ALTER TABLE meta.layer_registry
  ALTER COLUMN source_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'layer_registry_canonical_schema_table_key'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_canonical_schema_table_key
      UNIQUE (canonical_schema, canonical_table);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'source_registry_source_code_not_blank_chk'
      AND conrelid = 'meta.source_registry'::regclass
  ) THEN
    ALTER TABLE meta.source_registry
      ADD CONSTRAINT source_registry_source_code_not_blank_chk
      CHECK (btrim(source_code) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'source_registry_source_name_not_blank_chk'
      AND conrelid = 'meta.source_registry'::regclass
  ) THEN
    ALTER TABLE meta.source_registry
      ADD CONSTRAINT source_registry_source_name_not_blank_chk
      CHECK (btrim(source_name) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'source_registry_source_type_not_blank_chk'
      AND conrelid = 'meta.source_registry'::regclass
  ) THEN
    ALTER TABLE meta.source_registry
      ADD CONSTRAINT source_registry_source_type_not_blank_chk
      CHECK (btrim(source_type) <> '');
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_layer_key_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_layer_key_not_blank_chk
      CHECK (btrim(layer_key) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_layer_name_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_layer_name_not_blank_chk
      CHECK (btrim(layer_name) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_layer_family_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_layer_family_not_blank_chk
      CHECK (btrim(layer_family) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_source_path_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_source_path_not_blank_chk
      CHECK (source_path IS NULL OR btrim(source_path) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_canonical_schema_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_canonical_schema_not_blank_chk
      CHECK (btrim(canonical_schema) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'layer_registry_canonical_table_not_blank_chk'
      AND conrelid = 'meta.layer_registry'::regclass
  ) THEN
    ALTER TABLE meta.layer_registry
      ADD CONSTRAINT layer_registry_canonical_table_not_blank_chk
      CHECK (btrim(canonical_table) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'pipeline_runs_pipeline_name_not_blank_chk'
      AND conrelid = 'meta.pipeline_runs'::regclass
  ) THEN
    ALTER TABLE meta.pipeline_runs
      ADD CONSTRAINT pipeline_runs_pipeline_name_not_blank_chk
      CHECK (btrim(pipeline_name) <> '');
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Mandatory metadata on normalized vector tables
-- ---------------------------------------------------------------------------

DO $$
DECLARE
  r RECORD;
BEGIN
  -- Enforce NOT NULL on mandatory metadata columns across normalized vector tables.
  FOR r IN
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema IN ('buildings', 'transport', 'green')
      AND table_name LIKE '%\_normalized' ESCAPE '\'
      AND column_name IN ('city', 'layer_family', 'source_dataset', 'source_layer', 'source_file')
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.%I ALTER COLUMN %I SET NOT NULL',
      r.table_schema, r.table_name, r.column_name
    );
  END LOOP;

  -- Enforce non-blank text values for mandatory metadata.
  FOR r IN
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema IN ('buildings', 'transport', 'green')
      AND table_name LIKE '%\_normalized' ESCAPE '\'
      AND column_name IN ('city', 'layer_family', 'source_dataset', 'source_layer', 'source_file')
      AND data_type IN ('text', 'character varying')
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      WHERE n.nspname = r.table_schema
        AND t.relname = r.table_name
        AND c.conname = format(
          'ck_%s_%s_%s_not_blank',
          left(r.table_name, 18),
          r.column_name,
          substr(md5(r.table_schema || '.' || r.table_name || '.' || r.column_name), 1, 6)
        )
    ) THEN
      EXECUTE format(
        'ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (btrim(%I) <> '''')',
        r.table_schema,
        r.table_name,
        format(
          'ck_%s_%s_%s_not_blank',
          left(r.table_name, 18),
          r.column_name,
          substr(md5(r.table_schema || '.' || r.table_name || '.' || r.column_name), 1, 6)
        ),
        r.column_name
      );
    END IF;
  END LOOP;
END
$$;

-- Canonical ward boundaries must carry these identifiers.
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'boundaries'
      AND table_name LIKE '%\_wards\_normalized' ESCAPE '\'
      AND table_name NOT LIKE '%\_source\_normalized' ESCAPE '\'
      AND column_name IN ('city', 'ward_id', 'ward_name')
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.%I ALTER COLUMN %I SET NOT NULL',
      r.table_schema, r.table_name, r.column_name
    );
  END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- Metadata query performance indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_layer_registry_schema_table
  ON meta.layer_registry (canonical_schema, canonical_table);

CREATE INDEX IF NOT EXISTS idx_layer_registry_source
  ON meta.layer_registry (source_id);

CREATE INDEX IF NOT EXISTS idx_layer_registry_city_family
  ON meta.layer_registry (city, layer_family);

CREATE INDEX IF NOT EXISTS idx_layer_registry_validation_gin
  ON meta.layer_registry
  USING GIN (validation_state jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_layer_registry_provenance_gin
  ON meta.layer_registry
  USING GIN (provenance jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_source_registry_type_active
  ON meta.source_registry (source_type, is_active);

CREATE INDEX IF NOT EXISTS idx_metric_registry_category_group
  ON meta.metric_registry (category, frontend_group);

CREATE INDEX IF NOT EXISTS idx_metric_registry_source_layers_gin
  ON meta.metric_registry
  USING GIN (source_layers);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created
  ON meta.pipeline_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_city_scope_gin
  ON meta.pipeline_runs
  USING GIN (city_scope);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_output_layers_gin
  ON meta.pipeline_runs
  USING GIN (output_layers);

-- ---------------------------------------------------------------------------
-- Domain query performance indexes
-- ---------------------------------------------------------------------------

DO $$
DECLARE
  r RECORD;
  idx_name TEXT;
BEGIN
  -- Fast attribute filtering for ward identifiers.
  FOR r IN
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'boundaries'
      AND table_name LIKE '%\_wards\_normalized' ESCAPE '\'
      AND column_name IN ('ward_uid', 'ward_id', 'ward_name')
  LOOP
    idx_name := format(
      'idx_%s_%s_%s',
      left(r.table_name, 18),
      r.column_name,
      substr(md5(r.table_schema || '.' || r.table_name || '.' || r.column_name), 1, 6)
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I.%I (%I)',
      idx_name, r.table_schema, r.table_name, r.column_name
    );
  END LOOP;

  -- Fast filtering on normalized vector metadata and join keys.
  FOR r IN
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema IN ('buildings', 'transport', 'green')
      AND table_name LIKE '%\_normalized' ESCAPE '\'
      AND column_name IN ('source_layer', 'source_feature_id', 'ward_ref')
  LOOP
    idx_name := format(
      'idx_%s_%s_%s',
      left(r.table_name, 18),
      r.column_name,
      substr(md5(r.table_schema || '.' || r.table_name || '.' || r.column_name), 1, 6)
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I.%I (%I)',
      idx_name, r.table_schema, r.table_name, r.column_name
    );
  END LOOP;
END
$$;

-- Keep planner stats current after index/constraint changes.
ANALYZE;
