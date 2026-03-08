\set ON_ERROR_STOP on

-- Usage (psql):
--   psql -d postgres -f 001_init.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 001_init.sql

\if :{?db_name}
\else
\set db_name urbanmor
\endif

-- Create DB if missing (safe to re-run).
SELECT format('CREATE DATABASE %I', :'db_name')
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_database
  WHERE datname = :'db_name'
)\gexec

\connect :db_name

-- Spatial + text/search helpers used across ingestion and metrics workflows.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Domain schemas.
CREATE SCHEMA IF NOT EXISTS boundaries;
CREATE SCHEMA IF NOT EXISTS transport;
CREATE SCHEMA IF NOT EXISTS buildings;
CREATE SCHEMA IF NOT EXISTS lulc;
CREATE SCHEMA IF NOT EXISTS dem;
CREATE SCHEMA IF NOT EXISTS green;
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS meta;

-- Keep search_path explicit and stable.
ALTER DATABASE :"db_name" SET search_path = public, boundaries, transport, buildings, lulc, dem, green, metrics, meta;
