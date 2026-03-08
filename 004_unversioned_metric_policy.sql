\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f 004_unversioned_metric_policy.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 004_unversioned_metric_policy.sql

\if :{?db_name}
\connect :db_name
\endif

-- Collapse any legacy status values to unversioned labels.
UPDATE meta.metric_registry
SET status = CASE status
  WHEN 'implemented_v1' THEN 'implemented'
  WHEN 'planned_v1' THEN 'planned'
  WHEN 'planned_v1_1' THEN 'planned'
  WHEN 'planned_v1_2' THEN 'planned'
  WHEN 'planned_v1_3' THEN 'planned'
  WHEN 'planned_v2_plus' THEN 'planned'
  ELSE status
END,
release_target = 'final'
WHERE status IN (
  'implemented_v1',
  'planned_v1',
  'planned_v1_1',
  'planned_v1_2',
  'planned_v1_3',
  'planned_v2_plus'
) OR release_target <> 'final';

-- Replace status constraint.
ALTER TABLE meta.metric_registry
DROP CONSTRAINT IF EXISTS metric_registry_status_chk;

ALTER TABLE meta.metric_registry
ADD CONSTRAINT metric_registry_status_chk
CHECK (
  status IN (
    'implemented',
    'planned',
    'blocked_data',
    'proxy_only',
    'deprecated_or_revised'
  )
);

-- Enforce single non-version release target.
ALTER TABLE meta.metric_registry
DROP CONSTRAINT IF EXISTS metric_registry_release_target_final_chk;

ALTER TABLE meta.metric_registry
ADD CONSTRAINT metric_registry_release_target_final_chk
CHECK (release_target = 'final');
