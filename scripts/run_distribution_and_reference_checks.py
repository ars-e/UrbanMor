#!/usr/bin/env python3
from __future__ import annotations

import math
import os
import re
import subprocess
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('/Users/ars-e/projects/UrbanMor')
OUT = ROOT / 'output' / 'qa'
OUT.mkdir(parents=True, exist_ok=True)

DB_NAME = os.getenv('DB_NAME', 'urbanmor')

CITIES = [
    'ahmedabad',
    'bengaluru',
    'chandigarh',
    'chennai',
    'delhi',
    'kolkata',
    'mumbai',
]

OBSERVED_METRICS = [
    'lulc.green_cover_pct',
    'lulc.mix_index',
    'lulc.residential_cover_pct',
    'lulc.agriculture_pct',
    'lulc.water_coverage_pct',
    'open.bare_ground_pct',
    'open.vacant_land_pct',
    'topo.mean_elevation',
    'topo.elevation_range',
    'topo.mean_slope',
    'topo.steep_area_pct',
]

# Absolute + relative tolerance; effective tolerance is max(abs_tol, rel_tol*|reference|).
TOLERANCE_BANDS = {
    'lulc.green_cover_pct': {'abs_tol': 2.0, 'rel_tol': 0.04, 'unit': 'percentage_points'},
    'lulc.mix_index': {'abs_tol': 0.10, 'rel_tol': 0.10, 'unit': 'index_units'},
    'lulc.residential_cover_pct': {'abs_tol': 2.0, 'rel_tol': 0.05, 'unit': 'percentage_points'},
    'lulc.agriculture_pct': {'abs_tol': 2.0, 'rel_tol': 0.05, 'unit': 'percentage_points'},
    'lulc.water_coverage_pct': {'abs_tol': 1.0, 'rel_tol': 0.10, 'unit': 'percentage_points'},
    'open.bare_ground_pct': {'abs_tol': 1.0, 'rel_tol': 0.10, 'unit': 'percentage_points'},
    'open.vacant_land_pct': {'abs_tol': 2.5, 'rel_tol': 0.10, 'unit': 'percentage_points'},
    'topo.mean_elevation': {'abs_tol': 2.0, 'rel_tol': 0.03, 'unit': 'meters'},
    'topo.elevation_range': {'abs_tol': 3.0, 'rel_tol': 0.05, 'unit': 'meters'},
    'topo.mean_slope': {'abs_tol': 0.4, 'rel_tol': 0.08, 'unit': 'degrees'},
    'topo.steep_area_pct': {'abs_tol': 1.5, 'rel_tol': 0.15, 'unit': 'percentage_points'},
}


def normalize_ward_id(v: object) -> str:
    if v is None:
        return ''
    s = str(v).strip()
    if s == '' or s.lower() in {'nan', 'none', 'null'}:
        return ''
    if re.fullmatch(r'-?\d+\.0+', s):
        return s.split('.', 1)[0]
    try:
        f = float(s)
        if math.isfinite(f) and abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
    except ValueError:
        pass
    return s


def pick_join_key(city: str, ward_id_norm: str, ward_name_norm: str) -> str:
    if city in {'chennai', 'mumbai'} and ward_name_norm:
        return ward_name_norm
    return ward_id_norm


def run_sql_copy(sql: str) -> pd.DataFrame:
    cmd = [
        'psql',
        '-d',
        DB_NAME,
        '-v',
        'ON_ERROR_STOP=1',
        '-P',
        'pager=off',
        '-c',
        f'COPY ({sql}) TO STDOUT WITH CSV HEADER',
    ]
    env = os.environ.copy()
    env['PGOPTIONS'] = '-c statement_timeout=0'
    out = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
    return pd.read_csv(StringIO(out.stdout))


def fetch_observed_metrics() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for city in CITIES:
        q = f"""
            SELECT
              '{city}'::text AS city,
              ward_id::text AS ward_id,
              ward_name::text AS ward_name,
              metrics.compute_lulc_green_cover_pct('{city}', geom) AS lulc_green_cover_pct,
              metrics.compute_lulc_mix_index('{city}', geom) AS lulc_mix_index,
              metrics.compute_lulc_residential_cover_pct('{city}', geom) AS lulc_residential_cover_pct,
              metrics.compute_lulc_agriculture_pct('{city}', geom) AS lulc_agriculture_pct,
              metrics.compute_lulc_water_coverage_pct('{city}', geom) AS lulc_water_coverage_pct,
              metrics.compute_open_bare_ground_pct('{city}', geom) AS open_bare_ground_pct,
              metrics.compute_open_vacant_land_pct('{city}', geom) AS open_vacant_land_pct,
              metrics.compute_topo_mean_elevation('{city}', geom) AS topo_mean_elevation,
              metrics.compute_topo_elevation_range('{city}', geom) AS topo_elevation_range,
              metrics.compute_topo_mean_slope('{city}', geom) AS topo_mean_slope,
              metrics.compute_topo_steep_area_pct('{city}', geom) AS topo_steep_area_pct
            FROM boundaries.{city}_wards_normalized
        """
        df_city = run_sql_copy(q)
        frames.append(df_city)

    df = pd.concat(frames, ignore_index=True)
    rename = {
        'lulc_green_cover_pct': 'lulc.green_cover_pct',
        'lulc_mix_index': 'lulc.mix_index',
        'lulc_residential_cover_pct': 'lulc.residential_cover_pct',
        'lulc_agriculture_pct': 'lulc.agriculture_pct',
        'lulc_water_coverage_pct': 'lulc.water_coverage_pct',
        'open_bare_ground_pct': 'open.bare_ground_pct',
        'open_vacant_land_pct': 'open.vacant_land_pct',
        'topo_mean_elevation': 'topo.mean_elevation',
        'topo_elevation_range': 'topo.elevation_range',
        'topo_mean_slope': 'topo.mean_slope',
        'topo_steep_area_pct': 'topo.steep_area_pct',
    }
    df = df.rename(columns=rename)
    df['city'] = df['city'].str.strip().str.lower()
    df['ward_id_norm'] = df['ward_id'].map(normalize_ward_id)
    df['ward_name_norm'] = df['ward_name'].map(normalize_ward_id)
    df['join_key'] = [
        pick_join_key(city, ward_id_norm, ward_name_norm)
        for city, ward_id_norm, ward_name_norm in zip(df['city'], df['ward_id_norm'], df['ward_name_norm'])
    ]
    return df


def build_reference_lulc() -> pd.DataFrame:
    p = ROOT / 'output' / 'lulc_ward_stats_fullrun' / 'ward_lulc_stats_all_cities.csv'
    df = pd.read_csv(p)
    df['city'] = df['city'].astype(str).str.strip().str.lower()
    df['ward_id_norm'] = df['ward_id'].map(normalize_ward_id)

    area_cols = [c for c in df.columns if c.startswith('area_sqkm_class_')]
    for c in area_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df['total_area_sqkm'] = pd.to_numeric(df['total_area_sqkm'], errors='coerce')
    total = df['total_area_sqkm'].replace(0, np.nan)

    # Class map from current v1 assumptions.
    c = lambda k: pd.to_numeric(df.get(f'area_sqkm_class_{k}', 0), errors='coerce').fillna(0.0)
    green_sqkm = c(2) + c(4) + c(5) + c(11)
    water_sqkm = c(1)
    bare_sqkm = c(8)
    res_sqkm = c(7)
    agri_sqkm = c(5)
    vacant_sqkm = c(8) + c(11)

    # Shannon index on area share by class.
    areas = df[area_cols].fillna(0.0).to_numpy()
    totals = total.to_numpy()
    mix_vals: list[float] = []
    for i in range(len(df)):
        t = totals[i]
        if not np.isfinite(t) or t <= 0:
            mix_vals.append(np.nan)
            continue
        pvals = areas[i] / t
        pvals = pvals[pvals > 0]
        if pvals.size == 0:
            mix_vals.append(np.nan)
            continue
        mix_vals.append(float(-(pvals * np.log(pvals)).sum()))

    out = pd.DataFrame(
        {
            'city': df['city'],
            'ward_id_norm': df['ward_id_norm'],
            'join_key': df['ward_id_norm'],
            'lulc.green_cover_pct_ref': (green_sqkm / total) * 100.0,
            'lulc.mix_index_ref': mix_vals,
            'lulc.residential_cover_pct_ref': (res_sqkm / total) * 100.0,
            'lulc.agriculture_pct_ref': (agri_sqkm / total) * 100.0,
            'lulc.water_coverage_pct_ref': (water_sqkm / total) * 100.0,
            'open.bare_ground_pct_ref': (bare_sqkm / total) * 100.0,
            'open.vacant_land_pct_ref': (vacant_sqkm / total) * 100.0,
        }
    )
    return out


def build_reference_dem() -> pd.DataFrame:
    p = ROOT / 'output' / 'dem_terrain_ward_stats' / 'ward_dem_terrain_stats_all_cities.csv'
    df = pd.read_csv(p)
    df['city'] = df['city'].astype(str).str.strip().str.lower()
    df['ward_id_norm'] = df['ward_id'].map(normalize_ward_id)

    for c in ['elev_mean_m', 'elev_min_m', 'elev_max_m', 'slope_mean_deg', 'slope_gt_15deg_pct']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    out = pd.DataFrame(
        {
            'city': df['city'],
            'ward_id_norm': df['ward_id_norm'],
            'join_key': df['ward_id_norm'],
            'topo.mean_elevation_ref': df['elev_mean_m'],
            'topo.elevation_range_ref': df['elev_max_m'] - df['elev_min_m'],
            'topo.mean_slope_ref': df['slope_mean_deg'],
            'topo.steep_area_pct_ref': df['slope_gt_15deg_pct'],
        }
    )
    return out


def make_distribution_tables(df_obs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    long = df_obs.melt(
        id_vars=['city', 'ward_id_norm'],
        value_vars=OBSERVED_METRICS,
        var_name='metric_id',
        value_name='value',
    )
    long['value'] = pd.to_numeric(long['value'], errors='coerce')

    summary_rows: list[dict[str, object]] = []
    hist_rows: list[dict[str, object]] = []
    outlier_rows: list[dict[str, object]] = []

    for (city, metric_id), g in long.groupby(['city', 'metric_id'], dropna=False):
        vals = g['value'].dropna().astype(float)
        null_count = int(g['value'].isna().sum())
        n = int(vals.shape[0])

        if n == 0:
            summary_rows.append(
                {
                    'city': city,
                    'metric_id': metric_id,
                    'count_non_null': 0,
                    'count_null': null_count,
                    'min': np.nan,
                    'max': np.nan,
                    'mean': np.nan,
                    'std': np.nan,
                    'p50': np.nan,
                    'p95': np.nan,
                    'p99': np.nan,
                    'q1': np.nan,
                    'q3': np.nan,
                    'iqr': np.nan,
                }
            )
            continue

        q1 = float(np.percentile(vals, 25))
        q3 = float(np.percentile(vals, 75))
        iqr = q3 - q1
        p50 = float(np.percentile(vals, 50))
        p95 = float(np.percentile(vals, 95))
        p99 = float(np.percentile(vals, 99))
        med = float(np.median(vals))
        mad = float(np.median(np.abs(vals - med)))

        summary_rows.append(
            {
                'city': city,
                'metric_id': metric_id,
                'count_non_null': n,
                'count_null': null_count,
                'min': float(vals.min()),
                'max': float(vals.max()),
                'mean': float(vals.mean()),
                'std': float(vals.std(ddof=0)),
                'p50': p50,
                'p95': p95,
                'p99': p99,
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
            }
        )

        # Histograms.
        if metric_id.endswith('_pct'):
            bins = np.arange(0, 105, 5, dtype=float)
            if bins[-1] < 100:
                bins = np.append(bins, 100.0)
        else:
            vmin = float(vals.min())
            vmax = float(vals.max())
            if math.isclose(vmin, vmax):
                bins = np.array([vmin - 0.5, vmax + 0.5], dtype=float)
            else:
                bins = np.linspace(vmin, vmax, 21)

        counts, edges = np.histogram(vals, bins=bins)
        for i, cnt in enumerate(counts):
            hist_rows.append(
                {
                    'city': city,
                    'metric_id': metric_id,
                    'bin_left': float(edges[i]),
                    'bin_right': float(edges[i + 1]),
                    'count': int(cnt),
                }
            )

        lower_iqr = q1 - 3.0 * iqr
        upper_iqr = q3 + 3.0 * iqr

        for _, r in g.dropna(subset=['value']).iterrows():
            v = float(r['value'])
            impossible = False
            reasons: list[str] = []

            if metric_id.endswith('_pct') and (v < -1e-9 or v > 100.0 + 1e-9):
                impossible = True
                reasons.append('outside_0_100_pct_range')
            if metric_id == 'lulc.mix_index' and (v < -1e-9 or v > 3.0):
                impossible = True
                reasons.append('outside_mix_index_sanity_range')
            if metric_id == 'topo.mean_slope' and (v < -1e-9 or v > 90.0):
                impossible = True
                reasons.append('outside_slope_degree_range')
            if metric_id == 'topo.elevation_range' and v < -1e-9:
                impossible = True
                reasons.append('negative_elevation_range')
            if metric_id == 'topo.steep_area_pct' and (v < -1e-9 or v > 100.0 + 1e-9):
                impossible = True
                reasons.append('outside_0_100_pct_range')

            robust_z = np.nan
            if mad > 1e-12:
                robust_z = 0.6745 * (v - med) / mad

            extreme_iqr = (v < lower_iqr) or (v > upper_iqr)
            extreme_robust = np.isfinite(robust_z) and abs(robust_z) >= 8.0

            if impossible or extreme_iqr or extreme_robust:
                if extreme_iqr:
                    reasons.append('beyond_3x_iqr_envelope')
                if extreme_robust:
                    reasons.append('robust_z_ge_8')
                outlier_rows.append(
                    {
                        'city': city,
                        'ward_id_norm': r['ward_id_norm'],
                        'metric_id': metric_id,
                        'value': v,
                        'median_city_metric': med,
                        'q1': q1,
                        'q3': q3,
                        'iqr': iqr,
                        'robust_z': robust_z,
                        'is_impossible': bool(impossible),
                        'reason_flags': '|'.join(dict.fromkeys(reasons)),
                    }
                )

    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(hist_rows),
        pd.DataFrame(outlier_rows).sort_values(['city', 'metric_id', 'ward_id_norm']),
    )


def make_reference_crosscheck(df_obs: pd.DataFrame, ref_lulc: pd.DataFrame, ref_dem: pd.DataFrame) -> pd.DataFrame:
    ref = ref_lulc.merge(ref_dem, on=['city', 'ward_id_norm', 'join_key'], how='outer')

    obs_cols = ['city', 'ward_id_norm', 'ward_name_norm', 'join_key'] + OBSERVED_METRICS
    merged = df_obs[obs_cols].merge(ref, on=['city', 'join_key'], how='outer', suffixes=('_obs', '_refsrc'))

    rows: list[dict[str, object]] = []
    for metric in OBSERVED_METRICS:
        ref_col = f'{metric}_ref'
        if ref_col not in merged.columns:
            continue

        band = TOLERANCE_BANDS[metric]
        abs_tol = float(band['abs_tol'])
        rel_tol = float(band['rel_tol'])
        unit = band['unit']

        for _, r in merged.iterrows():
            obs = pd.to_numeric(pd.Series([r.get(metric)]), errors='coerce').iloc[0]
            exp = pd.to_numeric(pd.Series([r.get(ref_col)]), errors='coerce').iloc[0]

            if pd.isna(obs) and pd.isna(exp):
                status = 'both_missing'
                abs_diff = np.nan
                rel_diff = np.nan
                tol = np.nan
            elif pd.isna(obs):
                status = 'missing_observed'
                abs_diff = np.nan
                rel_diff = np.nan
                tol = max(abs_tol, rel_tol * (abs(float(exp)) if pd.notna(exp) else 0.0))
            elif pd.isna(exp):
                status = 'missing_reference'
                abs_diff = np.nan
                rel_diff = np.nan
                tol = np.nan
            else:
                obs_f = float(obs)
                exp_f = float(exp)
                abs_diff = abs(obs_f - exp_f)
                rel_diff = (abs_diff / abs(exp_f)) if abs(exp_f) > 1e-12 else (0.0 if abs_diff <= 1e-12 else np.inf)
                tol = max(abs_tol, rel_tol * abs(exp_f))
                status = 'pass' if abs_diff <= tol else 'fail'

            rows.append(
                {
                    'city': r.get('city'),
                    'ward_id_norm': (
                        r.get('ward_id_norm_obs')
                        if pd.notna(r.get('ward_id_norm_obs'))
                        else r.get('ward_id_norm_refsrc')
                    ),
                    'join_key': r.get('join_key'),
                    'metric_id': metric,
                    'observed_value': obs,
                    'reference_value': exp,
                    'abs_diff': abs_diff,
                    'rel_diff': rel_diff,
                    'abs_tolerance': abs_tol,
                    'rel_tolerance': rel_tol,
                    'effective_tolerance': tol,
                    'tolerance_unit': unit,
                    'status': status,
                }
            )

    out = pd.DataFrame(rows)
    return out.sort_values(['metric_id', 'city', 'ward_id_norm'])


def write_crosscheck_report(df_cross: pd.DataFrame, out_path: Path) -> None:
    def _markdown_table(df: pd.DataFrame) -> str:
        if df.empty:
            return 'No rows.'
        cols = list(df.columns)
        head = '| ' + ' | '.join(cols) + ' |'
        sep = '| ' + ' | '.join(['---'] * len(cols)) + ' |'
        rows: list[str] = []
        for _, rec in df.iterrows():
            vals: list[str] = []
            for c in cols:
                v = rec[c]
                if pd.isna(v):
                    vals.append('')
                elif isinstance(v, float):
                    vals.append(f'{v:.6g}')
                else:
                    vals.append(str(v))
            rows.append('| ' + ' | '.join(vals) + ' |')
        return '\n'.join([head, sep] + rows)

    stats = (
        df_cross.groupby(['metric_id', 'status'], dropna=False)
        .size()
        .rename('count')
        .reset_index()
    )

    metric_rollup = []
    for metric, g in df_cross.groupby('metric_id'):
        total = len(g)
        pass_n = int((g['status'] == 'pass').sum())
        fail_n = int((g['status'] == 'fail').sum())
        miss_obs = int((g['status'] == 'missing_observed').sum())
        miss_ref = int((g['status'] == 'missing_reference').sum())
        pass_rate = (pass_n / total * 100.0) if total else np.nan
        metric_rollup.append(
            {
                'metric_id': metric,
                'total_rows': total,
                'pass_rows': pass_n,
                'fail_rows': fail_n,
                'missing_observed_rows': miss_obs,
                'missing_reference_rows': miss_ref,
                'pass_rate_pct': pass_rate,
            }
        )
    roll_df = pd.DataFrame(metric_rollup).sort_values('metric_id')

    with out_path.open('w', encoding='utf-8') as f:
        f.write('# LULC/DEM Reference Cross-check Report\n\n')
        f.write('## Status Counts by Metric\n\n')
        if roll_df.empty:
            f.write('No rows.\n')
        else:
            f.write(_markdown_table(roll_df))
            f.write('\n\n')

        f.write('## Global Status Counts\n\n')
        if stats.empty:
            f.write('No rows.\n')
        else:
            global_stats = (
                df_cross['status']
                .value_counts(dropna=False)
                .rename_axis('status')
                .reset_index(name='count')
            )
            f.write(_markdown_table(global_stats))
            f.write('\n')


def main() -> int:
    print('Fetching observed ward-level metrics from PostGIS...')
    obs = fetch_observed_metrics()

    print('Building reference tables from CSVs...')
    ref_lulc = build_reference_lulc()
    ref_dem = build_reference_dem()

    print('Running distribution sanity checks...')
    summary_df, hist_df, outlier_df = make_distribution_tables(obs)

    print('Running LULC/DEM cross-check with tolerance bands...')
    cross_df = make_reference_crosscheck(obs, ref_lulc, ref_dem)

    tol_rows = [
        {
            'metric_id': m,
            'abs_tolerance': cfg['abs_tol'],
            'rel_tolerance': cfg['rel_tol'],
            'unit': cfg['unit'],
            'effective_tolerance_formula': 'max(abs_tolerance, rel_tolerance * abs(reference_value))',
        }
        for m, cfg in TOLERANCE_BANDS.items()
    ]
    tol_df = pd.DataFrame(tol_rows).sort_values('metric_id')

    # Persist artifacts.
    obs.to_csv(OUT / 'observed_metrics_lulc_topo_by_ward.csv', index=False)
    summary_df.to_csv(OUT / 'distribution_summary_by_city_metric.csv', index=False)
    hist_df.to_csv(OUT / 'distribution_histograms_by_city_metric.csv', index=False)
    outlier_df.to_csv(OUT / 'absurd_outliers_by_city_metric.csv', index=False)
    cross_df.to_csv(OUT / 'lulc_dem_reference_crosscheck.csv', index=False)
    tol_df.to_csv(OUT / 'lulc_dem_tolerance_bands.csv', index=False)

    write_crosscheck_report(cross_df, OUT / 'lulc_dem_crosscheck_report.md')

    # Small execution summary.
    fail_ct = int((cross_df['status'] == 'fail').sum()) if not cross_df.empty else 0
    pass_ct = int((cross_df['status'] == 'pass').sum()) if not cross_df.empty else 0
    out_ct = int(outlier_df.shape[0]) if not outlier_df.empty else 0
    print('Done.')
    print(f'Observed ward rows: {obs.shape[0]}')
    print(f'Cross-check pass: {pass_ct}, fail: {fail_ct}')
    print(f'Absurd outlier rows: {out_ct}')
    print(f'Artifacts written to: {OUT}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
