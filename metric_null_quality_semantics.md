# Metric Null & Quality Semantics

## Purpose
Standardize metric outputs so wards and user-drawn polygons return the same semantics.

## Output contract
Every metric result should include:

- `value`: numeric/object result or `null`
- `value_state`: one of `computed`, `null`, `not_computed`, `blocked_data`
- `method_state`: one of `direct`, `proxy_only`
- `is_zero`: boolean (true only when `value_state=computed` and value is numerically 0)
- `null_reason`: nullable string (`null_rule_triggered`, `calculator_not_implemented`, `blocked_by_unavailable_or_unusable_data`, etc.)
- `quality_flags`: string array

## Required distinctions

1. `0`
- Represented as `value=0`, `value_state=computed`, `is_zero=true`.
- Must never be converted to `null`.

2. `null`
- Represents a computed attempt that yielded no valid numeric/object output (for example null rule triggered).
- Represented as `value=null`, `value_state=null`.

3. `not_computed`
- Metric function not yet wired or intentionally skipped in run.
- Represented as `value=null`, `value_state=not_computed`.

4. `blocked_data`
- Metric cannot be computed due to unavailable/unusable prerequisites.
- Represented as `value=null`, `value_state=blocked_data`.

5. `proxy_only`
- Methodology-level designation, not a null state.
- Represented as `method_state=proxy_only` (with either computed or not_computed value_state).

## Quality flags taxonomy

- Confidence: `confidence_high`, `confidence_medium`, `confidence_low_medium`, `confidence_low`, `confidence_unknown`
- Prereq quality: `prereq_ok`, `prereq_caveated`, `prereq_missing`, `prereq_unknown`
- Runtime state: `computed`, `null_output`, `not_computed`, `blocked_data`
- Method label: `proxy_method`
- Value semantics: `zero_is_valid`

## Per-metric ledger
Machine-readable ledger is generated at:

- `metric_quality_flags.csv`

Columns:

- `metric_id`
- `metric_name`
- `home`
- `method_state`
- `default_value_state`
- `prereq_audit`
- `prereq_detail`
- `quality_flags`
- `feasibility`
- `implementation_status`
- `release_target`
- `backend_function`
- `notes`
