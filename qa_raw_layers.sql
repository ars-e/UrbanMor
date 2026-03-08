\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f qa_raw_layers.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f qa_raw_layers.sql

\if :{?db_name}
\connect :db_name
\endif

-- ---------------------------------------------------------------------------
-- Raw layer QA checks
--   - invalid geometries
--   - empty geometries
--   - missing CRS metadata
--   - duplicates (best available key)
--   - null required fields
--   - impossible bbox/extents
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS pg_temp.qa_vector_summary;
CREATE TEMP TABLE qa_vector_summary (
  table_schema TEXT,
  table_name TEXT,
  row_count BIGINT,
  invalid_geom_count BIGINT,
  empty_geom_count BIGINT,
  null_geom_count BIGINT,
  srid_missing_count BIGINT,
  impossible_bbox_count BIGINT,
  layer_extent TEXT,
  duplicate_key_column TEXT,
  duplicate_key_count BIGINT
);

DROP TABLE IF EXISTS pg_temp.qa_raster_summary;
CREATE TEMP TABLE qa_raster_summary (
  table_schema TEXT,
  table_name TEXT,
  tile_count BIGINT,
  empty_raster_count BIGINT,
  srid_missing_count BIGINT,
  impossible_bbox_count BIGINT,
  layer_extent TEXT
);

DROP TABLE IF EXISTS pg_temp.qa_required_nulls;
CREATE TEMP TABLE qa_required_nulls (
  table_schema TEXT,
  table_name TEXT,
  column_name TEXT,
  null_or_blank_count BIGINT
);

DROP TABLE IF EXISTS pg_temp.qa_crs_metadata;
CREATE TEMP TABLE qa_crs_metadata (
  table_schema TEXT,
  table_name TEXT,
  layer_type TEXT,
  metadata_srid INTEGER,
  metadata_issue TEXT
);

DO $$
DECLARE
  r RECORD;
  dup_col TEXT;
  q TEXT;
BEGIN
  -- ---------------------------
  -- Vector layer QA
  -- ---------------------------
  FOR r IN
    SELECT c.table_schema, c.table_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE c.column_name = 'geom'
      AND t.table_type = 'BASE TABLE'
      AND c.table_schema IN ('boundaries', 'transport', 'buildings', 'green')
    ORDER BY c.table_schema, c.table_name
  LOOP
    dup_col := NULL;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = r.table_schema
        AND table_name = r.table_name
        AND column_name = 'ward_uid'
    ) THEN
      dup_col := 'ward_uid';
    ELSIF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = r.table_schema
        AND table_name = r.table_name
        AND column_name = 'source_feature_id'
    ) THEN
      dup_col := 'source_feature_id';
    ELSIF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = r.table_schema
        AND table_name = r.table_name
        AND column_name = 'ward_id'
    ) THEN
      dup_col := 'ward_id';
    ELSIF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = r.table_schema
        AND table_name = r.table_name
        AND column_name = 'objectid'
    ) THEN
      dup_col := 'objectid';
    END IF;

    q := format(
      $fmt$
      WITH base AS (
        SELECT geom
        FROM %I.%I
      ),
      agg AS (
        SELECT
          count(*)::bigint AS row_count,
          count(*) FILTER (
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND NOT ST_IsValid(geom)
          )::bigint AS invalid_geom_count,
          count(*) FILTER (
            WHERE geom IS NOT NULL
              AND ST_IsEmpty(geom)
          )::bigint AS empty_geom_count,
          count(*) FILTER (
            WHERE geom IS NULL
          )::bigint AS null_geom_count,
          count(*) FILTER (
            WHERE geom IS NOT NULL
              AND ST_SRID(geom) = 0
          )::bigint AS srid_missing_count,
          count(*) FILTER (
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_SRID(geom) = 4326
              AND (
                ST_XMin(geom) < -180 OR ST_XMax(geom) > 180 OR
                ST_YMin(geom) < -90  OR ST_YMax(geom) > 90 OR
                ST_XMin(geom) > ST_XMax(geom) OR
                ST_YMin(geom) > ST_YMax(geom)
              )
          )::bigint AS impossible_bbox_count,
          ST_AsText(ST_Extent(geom)) AS layer_extent
        FROM base
      ),
      dup AS (
        %s
      )
      INSERT INTO qa_vector_summary (
        table_schema, table_name, row_count, invalid_geom_count, empty_geom_count,
        null_geom_count, srid_missing_count, impossible_bbox_count, layer_extent,
        duplicate_key_column, duplicate_key_count
      )
      SELECT
        %L, %L, agg.row_count, agg.invalid_geom_count, agg.empty_geom_count,
        agg.null_geom_count, agg.srid_missing_count, agg.impossible_bbox_count,
        agg.layer_extent, %s, dup.duplicate_key_count
      FROM agg, dup;
      $fmt$,
      r.table_schema,
      r.table_name,
      CASE
        WHEN dup_col IS NULL THEN
          'SELECT NULL::bigint AS duplicate_key_count'
        ELSE
          format(
            'SELECT COALESCE(sum(x.cnt - 1), 0)::bigint AS duplicate_key_count
             FROM (
               SELECT %I, count(*) AS cnt
               FROM %I.%I
               WHERE %I IS NOT NULL
               GROUP BY %I
               HAVING count(*) > 1
             ) x',
            dup_col, r.table_schema, r.table_name, dup_col, dup_col
          )
      END,
      r.table_schema,
      r.table_name,
      CASE WHEN dup_col IS NULL THEN 'NULL' ELSE quote_literal(dup_col) END
    );
    EXECUTE q;

    -- metadata-level CRS check (vector)
    INSERT INTO qa_crs_metadata (table_schema, table_name, layer_type, metadata_srid, metadata_issue)
    SELECT
      r.table_schema,
      r.table_name,
      'vector',
      gc.srid,
      CASE
        WHEN gc.srid IS NULL THEN 'missing_in_geometry_columns'
        WHEN gc.srid = 0 THEN 'undefined_srid'
        ELSE NULL
      END
    FROM geometry_columns gc
    WHERE gc.f_table_schema = r.table_schema
      AND gc.f_table_name = r.table_name
      AND gc.f_geometry_column = 'geom'
      AND (gc.srid IS NULL OR gc.srid = 0);
  END LOOP;

  -- ---------------------------
  -- Raster layer QA
  -- ---------------------------
  FOR r IN
    SELECT c.table_schema, c.table_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE c.column_name = 'rast'
      AND t.table_type = 'BASE TABLE'
      AND c.table_schema IN ('dem', 'lulc')
    ORDER BY c.table_schema, c.table_name
  LOOP
    q := format(
      $fmt$
      WITH base AS (
        SELECT rast
        FROM %I.%I
      )
      INSERT INTO qa_raster_summary (
        table_schema, table_name, tile_count, empty_raster_count, srid_missing_count,
        impossible_bbox_count, layer_extent
      )
      SELECT
        %L,
        %L,
        count(*)::bigint AS tile_count,
        count(*) FILTER (WHERE ST_IsEmpty(rast))::bigint AS empty_raster_count,
        count(*) FILTER (WHERE ST_SRID(rast) = 0)::bigint AS srid_missing_count,
        count(*) FILTER (
          WHERE ST_SRID(rast) = 4326 AND (
            ST_XMin(ST_Envelope(rast)) < -180 OR ST_XMax(ST_Envelope(rast)) > 180 OR
            ST_YMin(ST_Envelope(rast)) < -90  OR ST_YMax(ST_Envelope(rast)) > 90 OR
            ST_XMin(ST_Envelope(rast)) > ST_XMax(ST_Envelope(rast)) OR
            ST_YMin(ST_Envelope(rast)) > ST_YMax(ST_Envelope(rast))
          )
        )::bigint AS impossible_bbox_count,
        ST_AsText(ST_Extent(ST_Envelope(rast))) AS layer_extent
      FROM base;
      $fmt$,
      r.table_schema, r.table_name, r.table_schema, r.table_name
    );
    EXECUTE q;

    -- metadata-level CRS check (raster)
    INSERT INTO qa_crs_metadata (table_schema, table_name, layer_type, metadata_srid, metadata_issue)
    SELECT
      r.table_schema,
      r.table_name,
      'raster',
      rc.srid,
      CASE
        WHEN rc.srid IS NULL THEN 'missing_in_raster_columns'
        WHEN rc.srid = 0 THEN 'undefined_srid'
        ELSE NULL
      END
    FROM raster_columns rc
    WHERE rc.r_table_schema = r.table_schema
      AND rc.r_table_name = r.table_name
      AND rc.r_raster_column = 'rast'
      AND (rc.srid IS NULL OR rc.srid = 0);
  END LOOP;

  -- ---------------------------
  -- Required-field null checks
  -- ---------------------------

  -- Normalized vector metadata columns.
  FOR r IN
    SELECT c.table_schema, c.table_name, c.column_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE t.table_type = 'BASE TABLE'
      AND c.table_schema IN ('buildings', 'transport', 'green')
      AND c.table_name LIKE '%\_normalized' ESCAPE '\'
      AND c.column_name IN ('city', 'layer_family', 'source_dataset', 'source_layer', 'source_file')
    ORDER BY c.table_schema, c.table_name, c.column_name
  LOOP
    q := format(
      $fmt$
      INSERT INTO qa_required_nulls (table_schema, table_name, column_name, null_or_blank_count)
      SELECT
        %L,
        %L,
        %L,
        count(*) FILTER (WHERE %I IS NULL OR btrim(%I) = '')::bigint
      FROM %I.%I;
      $fmt$,
      r.table_schema, r.table_name, r.column_name,
      r.column_name, r.column_name, r.table_schema, r.table_name
    );
    EXECUTE q;
  END LOOP;

  -- Canonical ward required columns.
  FOR r IN
    SELECT c.table_schema, c.table_name, c.column_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE t.table_type = 'BASE TABLE'
      AND c.table_schema = 'boundaries'
      AND c.table_name LIKE '%\_wards\_normalized' ESCAPE '\'
      AND c.table_name NOT LIKE '%\_source\_normalized' ESCAPE '\'
      AND c.column_name IN ('city', 'ward_id', 'ward_name')
    ORDER BY c.table_schema, c.table_name, c.column_name
  LOOP
    q := format(
      $fmt$
      INSERT INTO qa_required_nulls (table_schema, table_name, column_name, null_or_blank_count)
      SELECT
        %L,
        %L,
        %L,
        count(*) FILTER (WHERE %I IS NULL OR btrim(%I) = '')::bigint
      FROM %I.%I;
      $fmt$,
      r.table_schema, r.table_name, r.column_name,
      r.column_name, r.column_name, r.table_schema, r.table_name
    );
    EXECUTE q;
  END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- Output reports
-- ---------------------------------------------------------------------------

-- 1) Vector QA summary
SELECT
  table_schema,
  table_name,
  row_count,
  invalid_geom_count,
  empty_geom_count,
  null_geom_count,
  srid_missing_count,
  impossible_bbox_count,
  duplicate_key_column,
  duplicate_key_count,
  layer_extent
FROM qa_vector_summary
ORDER BY table_schema, table_name;

-- 2) Raster QA summary
SELECT
  table_schema,
  table_name,
  tile_count,
  empty_raster_count,
  srid_missing_count,
  impossible_bbox_count,
  layer_extent
FROM qa_raster_summary
ORDER BY table_schema, table_name;

-- 3) Required fields null/blank report
SELECT
  table_schema,
  table_name,
  column_name,
  null_or_blank_count
FROM qa_required_nulls
ORDER BY table_schema, table_name, column_name;

-- 4) CRS metadata issues (empty result = pass)
SELECT
  table_schema,
  table_name,
  layer_type,
  metadata_srid,
  metadata_issue
FROM qa_crs_metadata
ORDER BY table_schema, table_name, layer_type;

-- 5) Rollup severity (quick scan)
SELECT
  'vector' AS layer_type,
  sum(invalid_geom_count + empty_geom_count + null_geom_count + srid_missing_count + impossible_bbox_count + COALESCE(duplicate_key_count, 0)) AS total_issues
FROM qa_vector_summary
UNION ALL
SELECT
  'raster' AS layer_type,
  sum(empty_raster_count + srid_missing_count + impossible_bbox_count) AS total_issues
FROM qa_raster_summary
UNION ALL
SELECT
  'required_fields' AS layer_type,
  sum(null_or_blank_count) AS total_issues
FROM qa_required_nulls
UNION ALL
SELECT
  'crs_metadata' AS layer_type,
  count(*)::bigint AS total_issues
FROM qa_crs_metadata;
