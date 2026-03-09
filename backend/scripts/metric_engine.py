#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
COVERAGE_MATRIX = ROOT / 'metric_coverage_matrix.csv'

# Prefix -> canonical family function home.
FAMILY_PREFIX_TO_HOME = {
    'road': 'roads',
    'transit': 'roads',
    'bldg': 'buildings',
    'lulc': 'landuse',
    'open': 'landuse',
    'topo': 'topography',
    'cmp': 'composites',
}

HOME_TO_FUNCTION = {
    'roads': 'analyse_roads',
    'buildings': 'analyse_buildings',
    'landuse': 'analyse_landuse',
    'topography': 'analyse_topography',
    'composites': 'analyse_composites',
}


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    metric_name: str
    family: str
    backend_function: str
    implementation_status: str
    feasibility: str
    release_target: str
    notes: str
    frontend_visible: bool


@dataclass
class MetricResult:
    metric_id: str
    value: Any
    value_state: str
    method_state: str
    is_zero: bool
    null_reason: str | None
    prereq_audit: str
    prereq_detail: str | None
    quality_flags: list[str]
    home: str
    backend_function: str
    implementation_status: str
    feasibility: str
    release_target: str
    notes: str


Calculator = Callable[[Any, MetricDefinition, dict[str, Any]], Any]


class MetricEngine:
    def __init__(
        self,
        coverage_matrix: Path = COVERAGE_MATRIX,
        calculators: dict[str, Calculator] | None = None,
    ) -> None:
        self.coverage_matrix = coverage_matrix
        self.calculators = calculators or {}
        self.metrics = self._load_metrics()
        self.metrics_by_home = self._group_metrics_by_home()

    def _load_metrics(self) -> list[MetricDefinition]:
        if not self.coverage_matrix.exists():
            raise FileNotFoundError(f'Missing coverage matrix: {self.coverage_matrix}')

        metrics: list[MetricDefinition] = []
        with self.coverage_matrix.open('r', encoding='utf-8', newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                metrics.append(
                    MetricDefinition(
                        metric_id=row['metric_id'].strip(),
                        metric_name=row['metric_name'].strip(),
                        family=row['family'].strip(),
                        backend_function=row['backend_function'].strip(),
                        implementation_status=row['implementation_status'].strip(),
                        feasibility=row['feasibility'].strip(),
                        release_target=row['release_target'].strip(),
                        notes=row.get('notes', '').strip(),
                        frontend_visible=str(row.get('frontend_visible', '')).strip().lower() == 'true',
                    )
                )
        return metrics

    @staticmethod
    def _metric_home(metric_id: str) -> str:
        prefix = metric_id.split('.', 1)[0].strip().lower()
        home = FAMILY_PREFIX_TO_HOME.get(prefix)
        if home is None:
            raise ValueError(f'Metric has no family home: {metric_id}')
        return home

    def _group_metrics_by_home(self) -> dict[str, list[MetricDefinition]]:
        grouped: dict[str, list[MetricDefinition]] = {k: [] for k in HOME_TO_FUNCTION}
        for metric in self.metrics:
            home = self._metric_home(metric.metric_id)
            grouped[home].append(metric)

        # Hard guard: every metric must have a home and every home list is deterministic.
        for home in grouped:
            grouped[home] = sorted(grouped[home], key=lambda x: x.metric_id)
        return grouped

    @staticmethod
    def _extract_prereq_audit(notes: str) -> tuple[str, str | None]:
        m = re.search(r'prereq_audit=([a-z_]+)(?::([^|]+))?', notes)
        if not m:
            return 'unknown', None
        status = m.group(1).strip().lower()
        detail = m.group(2).strip() if m.group(2) else None
        return status, detail

    @staticmethod
    def _method_state(metric: MetricDefinition) -> str:
        return 'proxy_only' if metric.implementation_status == 'proxy_only' else 'direct'

    @staticmethod
    def _confidence_flag(metric: MetricDefinition) -> str:
        mapping = {
            'High': 'confidence_high',
            'Medium': 'confidence_medium',
            'Low-Medium': 'confidence_low_medium',
            'Low': 'confidence_low',
        }
        return mapping.get(metric.feasibility, 'confidence_unknown')

    def _quality_flags(
        self,
        metric: MetricDefinition,
        *,
        value_state: str,
        method_state: str,
        is_zero: bool,
        prereq_audit: str,
    ) -> list[str]:
        flags: list[str] = [self._confidence_flag(metric)]

        if method_state == 'proxy_only':
            flags.append('proxy_method')

        if value_state == 'not_computed':
            flags.append('not_computed')
        elif value_state == 'blocked_data':
            flags.append('blocked_data')
        elif value_state == 'null':
            flags.append('null_output')
        elif value_state == 'computed':
            flags.append('computed')

        if is_zero:
            flags.append('zero_is_valid')

        if prereq_audit == 'ok':
            flags.append('prereq_ok')
        elif prereq_audit == 'usable_with_caveats':
            flags.append('prereq_caveated')
        elif prereq_audit == 'missing':
            flags.append('prereq_missing')
        else:
            flags.append('prereq_unknown')

        # Preserve order but remove duplicates.
        seen: set[str] = set()
        deduped: list[str] = []
        for f in flags:
            if f not in seen:
                seen.add(f)
                deduped.append(f)
        return deduped

    @staticmethod
    def _classify_value_state(
        metric: MetricDefinition,
        *,
        has_calculator: bool,
        value: Any,
    ) -> tuple[str, str | None]:
        if metric.implementation_status == 'blocked_data':
            return 'blocked_data', 'blocked_by_unavailable_or_unusable_data'
        if not has_calculator:
            return 'not_computed', 'calculator_not_implemented'
        if value is None:
            return 'null', 'null_rule_triggered'
        if isinstance(value, float) and math.isnan(value):
            return 'null', 'nan_coerced_to_null'
        return 'computed', None

    def metric_home_summary(self) -> dict[str, Any]:
        missing_home: list[str] = []
        by_home = {home: len(items) for home, items in self.metrics_by_home.items()}
        total_counted = sum(by_home.values())

        for m in self.metrics:
            try:
                self._metric_home(m.metric_id)
            except ValueError:
                missing_home.append(m.metric_id)

        return {
            'total_metrics': len(self.metrics),
            'total_counted': total_counted,
            'by_home': by_home,
            'missing_home_metrics': sorted(missing_home),
        }

    def _default_result(self, metric: MetricDefinition, home: str) -> MetricResult:
        # If no calculator is wired yet, return a structured placeholder result.
        prereq_audit, prereq_detail = self._extract_prereq_audit(metric.notes)
        method_state = self._method_state(metric)
        value_state, null_reason = self._classify_value_state(
            metric,
            has_calculator=False,
            value=None,
        )
        quality_flags = self._quality_flags(
            metric,
            value_state=value_state,
            method_state=method_state,
            is_zero=False,
            prereq_audit=prereq_audit,
        )
        return MetricResult(
            metric_id=metric.metric_id,
            value=None,
            value_state=value_state,
            method_state=method_state,
            is_zero=False,
            null_reason=null_reason,
            prereq_audit=prereq_audit,
            prereq_detail=prereq_detail,
            quality_flags=quality_flags,
            home=home,
            backend_function=metric.backend_function,
            implementation_status=metric.implementation_status,
            feasibility=metric.feasibility,
            release_target=metric.release_target,
            notes=metric.notes,
        )

    def _run_home(
        self,
        home: str,
        geom: Any,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        payload = payload or {}
        out: dict[str, dict[str, Any]] = {}
        metrics = self.metrics_by_home[home]

        for metric in metrics:
            calc = self.calculators.get(metric.backend_function)
            if calc is None:
                result = self._default_result(metric, home)
            else:
                prereq_audit, prereq_detail = self._extract_prereq_audit(metric.notes)
                method_state = self._method_state(metric)
                raw_value = calc(geom, metric, payload)
                value_state, null_reason = self._classify_value_state(
                    metric,
                    has_calculator=True,
                    value=raw_value,
                )
                value = None if value_state in {'null', 'not_computed', 'blocked_data'} else raw_value
                is_zero = (
                    value_state == 'computed'
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and float(value) == 0.0
                )
                quality_flags = self._quality_flags(
                    metric,
                    value_state=value_state,
                    method_state=method_state,
                    is_zero=is_zero,
                    prereq_audit=prereq_audit,
                )
                result = MetricResult(
                    metric_id=metric.metric_id,
                    value=value,
                    value_state=value_state,
                    method_state=method_state,
                    is_zero=is_zero,
                    null_reason=null_reason,
                    prereq_audit=prereq_audit,
                    prereq_detail=prereq_detail,
                    quality_flags=quality_flags,
                    home=home,
                    backend_function=metric.backend_function,
                    implementation_status=metric.implementation_status,
                    feasibility=metric.feasibility,
                    release_target=metric.release_target,
                    notes=metric.notes,
                )
            out[result.metric_id] = asdict(result)

        return out

    def analyse_roads(self, geom: Any) -> dict[str, dict[str, Any]]:
        return self._run_home('roads', geom)

    def analyse_buildings(self, geom: Any) -> dict[str, dict[str, Any]]:
        return self._run_home('buildings', geom)

    def analyse_landuse(self, geom: Any) -> dict[str, dict[str, Any]]:
        return self._run_home('landuse', geom)

    def analyse_topography(self, geom: Any) -> dict[str, dict[str, Any]]:
        return self._run_home('topography', geom)

    def analyse_composites(self, metric_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        geom = metric_payload.get('geom')
        return self._run_home('composites', geom, payload=metric_payload)

    def analyse_polygon(self, geom: Any) -> dict[str, Any]:
        roads = self.analyse_roads(geom)
        buildings = self.analyse_buildings(geom)
        landuse = self.analyse_landuse(geom)
        topography = self.analyse_topography(geom)

        composite_payload = {
            'geom': geom,
            'roads': roads,
            'buildings': buildings,
            'landuse': landuse,
            'topography': topography,
        }
        composites = self.analyse_composites(composite_payload)

        all_metrics: dict[str, dict[str, Any]] = {}
        all_metrics.update(roads)
        all_metrics.update(buildings)
        all_metrics.update(landuse)
        all_metrics.update(topography)
        all_metrics.update(composites)

        summary = self.metric_home_summary()
        summary['returned_metrics'] = len(all_metrics)

        return {
            'families': {
                'roads': roads,
                'buildings': buildings,
                'landuse': landuse,
                'topography': topography,
                'composites': composites,
            },
            'all_metrics': all_metrics,
            'summary': summary,
        }

    def export_metric_quality_flags(self, out_csv: Path) -> dict[str, Any]:
        out_csv.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            'metric_id',
            'metric_name',
            'home',
            'method_state',
            'default_value_state',
            'prereq_audit',
            'prereq_detail',
            'quality_flags',
            'feasibility',
            'implementation_status',
            'release_target',
            'backend_function',
            'notes',
        ]

        with out_csv.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for metric in sorted(self.metrics, key=lambda x: x.metric_id):
                home = self._metric_home(metric.metric_id)
                method_state = self._method_state(metric)
                prereq_audit, prereq_detail = self._extract_prereq_audit(metric.notes)
                default_value_state, _ = self._classify_value_state(
                    metric,
                    has_calculator=False,
                    value=None,
                )
                flags = self._quality_flags(
                    metric,
                    value_state=default_value_state,
                    method_state=method_state,
                    is_zero=False,
                    prereq_audit=prereq_audit,
                )
                writer.writerow(
                    {
                        'metric_id': metric.metric_id,
                        'metric_name': metric.metric_name,
                        'home': home,
                        'method_state': method_state,
                        'default_value_state': default_value_state,
                        'prereq_audit': prereq_audit,
                        'prereq_detail': prereq_detail or '',
                        'quality_flags': '|'.join(flags),
                        'feasibility': metric.feasibility,
                        'implementation_status': metric.implementation_status,
                        'release_target': metric.release_target,
                        'backend_function': metric.backend_function,
                        'notes': metric.notes,
                    }
                )

        return {
            'output_path': str(out_csv),
            'row_count': len(self.metrics),
        }


_DEFAULT_ENGINE = MetricEngine()


def analyse_roads(geom: Any) -> dict[str, dict[str, Any]]:
    return _DEFAULT_ENGINE.analyse_roads(geom)


def analyse_buildings(geom: Any) -> dict[str, dict[str, Any]]:
    return _DEFAULT_ENGINE.analyse_buildings(geom)


def analyse_landuse(geom: Any) -> dict[str, dict[str, Any]]:
    return _DEFAULT_ENGINE.analyse_landuse(geom)


def analyse_topography(geom: Any) -> dict[str, dict[str, Any]]:
    return _DEFAULT_ENGINE.analyse_topography(geom)


def analyse_composites(metric_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return _DEFAULT_ENGINE.analyse_composites(metric_payload)


def analyse_polygon(geom: Any) -> dict[str, Any]:
    return _DEFAULT_ENGINE.analyse_polygon(geom)


def _cli() -> None:
    parser = argparse.ArgumentParser(description='UrbanMor metric family engine')
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print metric home coverage summary and exit.',
    )
    parser.add_argument(
        '--emit-quality-flags',
        default=None,
        help='Write per-metric quality flag ledger CSV to this path.',
    )
    args = parser.parse_args()

    if args.summary:
        print(json.dumps(_DEFAULT_ENGINE.metric_home_summary(), indent=2, sort_keys=True))
        return

    if args.emit_quality_flags:
        report = _DEFAULT_ENGINE.export_metric_quality_flags(Path(args.emit_quality_flags))
        print(json.dumps(report, indent=2, sort_keys=True))
        return

    demo = analyse_polygon(geom={'type': 'Polygon'})
    print(json.dumps(demo['summary'], indent=2, sort_keys=True))


if __name__ == '__main__':
    _cli()
