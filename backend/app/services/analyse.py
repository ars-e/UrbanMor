import asyncio
import json
import logging
import math
import re
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session_factory
from app.observability import OBS
from app.schemas.analysis import AnalyseJobResponse, AnalyseRequest, AnalyseResponse
from app.schemas.cities import (
    CitiesResponse,
    CityMetricsResponse,
    CitySummary,
    CityWardsGeoJSONResponse,
    CityWardsResponse,
    MetricAggregate,
    WardFeature,
    WardFeatureProperties,
    WardSummary,
)
from app.schemas.health import HealthResponse
from app.schemas.map_layers import CityMapLayerGeoJSONResponse, MapLayerFeature
from app.schemas.meta import MetaMetricsResponse, MetricMetaItem
from app.schemas.metrics import WardMetricResponse

logger = logging.getLogger("urbanmor.service.analyse")
_CITY_RE = re.compile(r"^[a-z0-9_]+$")
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()
_JOB_SEMAPHORE = asyncio.Semaphore(2)
_JOBS_TABLE_READY = False
_JOBS_TABLE_LOCK = asyncio.Lock()
_TRANSIT_POINT_LAYERS = (
    "metro_stations",
    "rail_stations",
    "public_transport_stations",
    "public_transport_stops",
    "public_transport_platforms",
    "metro_entrances",
)
_TRANSIT_POINT_LAYER_SQL = ", ".join(f"'{layer}'" for layer in _TRANSIT_POINT_LAYERS)
_RETIRED_METRIC_IDS = frozenset({"bldg.growth_rate", "topo.flood_risk_proxy"})
_JOB_RUNNING_RECLAIM_AFTER_MINUTES = 20
_JOB_STALE_QUEUE_MINUTES = 30
_CUSTOM_ANALYSIS_STATEMENT_TIMEOUT = "180s"
_STALE_EXPIRY_INTERVAL_SECONDS = 30
_LAST_STALE_EXPIRY_MONO = 0.0
_STALE_EXPIRY_LOCK = asyncio.Lock()

# Composite metrics are backfilled in API responses so stale ward-cache rows
# (missing cmp.* keys) still return a complete metric payload.
_COMPOSITE_METRIC_IDS = (
    "cmp.walkability_index",
    "cmp.informality_index",
    "cmp.heat_island_proxy",
    "cmp.development_pressure",
    "cmp.topographic_constraint_expansion",
    "cmp.green_accessibility",
    "cmp.transit_access_green",
    "cmp.compactness",
)


def _cleanup_background_task(task: asyncio.Task[Any]) -> None:
    _BACKGROUND_TASKS.discard(task)
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc is not None:
        logger.exception("analysis_background_task_failed", exc_info=exc)


async def run_custom_polygon_job(job_id: str) -> None:
    async with _JOB_SEMAPHORE:
        session_factory = get_session_factory()
        async with session_factory() as session:
            service = AnalyseService(session)
            await service.run_custom_polygon_job(job_id)


class AnalyseService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _execute(self, label: str, sql: str, params: dict[str, Any] | None = None):
        start = time.perf_counter()
        try:
            result = await self.session.execute(text(sql), params or {})
            return result
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            OBS.record_query(label, duration_ms)
            logger.info("query_timing label=%s duration_ms=%.3f", label, duration_ms)

    async def _scalar(self, label: str, sql: str, params: dict[str, Any] | None = None) -> Any:
        result = await self._execute(label, sql, params)
        return result.scalar()

    @staticmethod
    def _normalize_city(city: str) -> str:
        normalized = city.lower().strip()
        if not _CITY_RE.match(normalized):
            raise HTTPException(status_code=400, detail="Invalid city format")
        return normalized

    @staticmethod
    def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
        try:
            west, south, east, north = (float(part.strip()) for part in bbox.split(","))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north") from exc

        if not (-180.0 <= west <= 180.0 and -180.0 <= east <= 180.0 and -90.0 <= south <= 90.0 and -90.0 <= north <= 90.0):
            raise HTTPException(status_code=400, detail="bbox coordinates out of range")
        if west >= east or south >= north:
            raise HTTPException(status_code=400, detail="bbox must satisfy west < east and south < north")
        return west, south, east, north

    @staticmethod
    def _road_detail(detail: str | None, zoom: float | None) -> str:
        if detail in {"major", "full"}:
            return detail
        if zoom is not None and zoom >= 12:
            return "full"
        return "major"

    @staticmethod
    def _road_simplify_tolerance_m(zoom: float | None) -> float:
        if zoom is None:
            return 20.0
        if zoom >= 14:
            return 1.5
        if zoom >= 12:
            return 4.0
        if zoom >= 10:
            return 12.0
        return 30.0

    @staticmethod
    def _metric_number(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
            if math.isfinite(numeric):
                return numeric
        return None

    @staticmethod
    def _clamp_0_100(value: float | None) -> float | None:
        if value is None or not math.isfinite(value):
            return None
        if value < 0.0:
            return 0.0
        if value > 100.0:
            return 100.0
        return value

    @classmethod
    def _compute_composite_metrics(cls, metrics: dict[str, Any]) -> dict[str, float | None]:
        def m(metric_id: str) -> float | None:
            return cls._metric_number(metrics.get(metric_id))

        def all_missing(*values: float | None) -> bool:
            return all(value is None for value in values)

        road_intersection_density = m("road.intersection_density")
        road_cnr = m("road.cnr")
        road_ped_ratio = m("road.pedestrian_infra_ratio")
        transit_coverage = m("transit.coverage_500m")
        transit_distance_m = m("transit.distance_to_metro_or_rail")

        if all_missing(road_intersection_density, road_cnr, road_ped_ratio, transit_coverage, transit_distance_m):
            walkability = None
        else:
            intersection_norm = cls._clamp_0_100(((road_intersection_density or 0.0) / 120.0) * 100.0) or 0.0
            transit_distance_score = 0.0 if transit_distance_m is None else 100.0 / (1.0 + (transit_distance_m / 500.0))
            walkability = cls._clamp_0_100(
                (0.25 * intersection_norm)
                + (0.25 * (road_cnr or 0.0))
                + (0.20 * (road_ped_ratio or 0.0))
                + (0.20 * (transit_coverage or 0.0))
                + (0.10 * transit_distance_score)
            )

        bldg_density = m("bldg.density_per_ha")
        green_cover = m("lulc.green_cover_pct")
        vacant_pct = m("open.vacant_land_pct")
        if all_missing(bldg_density, road_cnr, green_cover, vacant_pct):
            informality = None
        else:
            density_norm = cls._clamp_0_100(((bldg_density or 0.0) / 250.0) * 100.0) or 0.0
            informality = cls._clamp_0_100(
                (0.35 * density_norm)
                + (0.25 * (100.0 - (road_cnr or 0.0)))
                + (0.20 * (100.0 - (green_cover or 0.0)))
                + (0.20 * (vacant_pct or 0.0))
            )

        impervious = m("lulc.impervious_ratio")
        bcr = m("bldg.bcr")
        flat_area = m("topo.flat_area_pct")
        if all_missing(impervious, bcr, green_cover, flat_area):
            heat_island = None
        else:
            heat_island = cls._clamp_0_100(
                (0.35 * (impervious or 0.0))
                + (0.25 * (bcr or 0.0))
                + (0.25 * (100.0 - (green_cover or 0.0)))
                + (0.15 * (flat_area or 0.0))
            )

        road_edge_density = m("road.edge_density")
        if all_missing(vacant_pct, bldg_density, road_edge_density):
            development_pressure = None
        else:
            density_norm = cls._clamp_0_100(((bldg_density or 0.0) / 250.0) * 100.0) or 0.0
            edge_norm = cls._clamp_0_100(((road_edge_density or 0.0) / 30.0) * 100.0) or 0.0
            development_pressure = cls._clamp_0_100(
                (0.40 * (vacant_pct or 0.0))
                + (0.35 * density_norm)
                + (0.25 * edge_norm)
            )

        topo_natural_constraint = m("topo.natural_constraint_index")
        topo_steep_pct = m("topo.steep_area_pct")
        if all_missing(topo_natural_constraint, topo_steep_pct):
            topographic_constraint = None
        else:
            # Flood-risk proxy was retired; weights are renormalized from 0.5/0.3.
            topographic_constraint = cls._clamp_0_100(
                (0.625 * (topo_natural_constraint or 0.0))
                + (0.375 * (topo_steep_pct or 0.0))
            )

        distance_to_park_m = m("open.distance_to_nearest_park")
        park_density = m("open.park_green_space_density")
        if all_missing(distance_to_park_m, park_density, green_cover):
            green_accessibility = None
        else:
            distance_score = 0.0 if distance_to_park_m is None else 100.0 / (1.0 + (distance_to_park_m / 300.0))
            park_density_norm = cls._clamp_0_100(((park_density or 0.0) / 80.0) * 100.0) or 0.0
            green_accessibility = cls._clamp_0_100(
                (0.40 * distance_score)
                + (0.30 * park_density_norm)
                + (0.30 * (green_cover or 0.0))
            )

        if all_missing(transit_coverage, green_accessibility):
            transit_access_green = None
        else:
            transit_access_green = cls._clamp_0_100(
                (0.55 * (transit_coverage or 0.0))
                + (0.45 * (green_accessibility or 0.0))
            )

        lulc_mix_index = m("lulc.mix_index")
        road_circuity = m("road.circuity")
        if all_missing(bcr, road_intersection_density, lulc_mix_index, road_circuity):
            compactness = None
        else:
            intersection_norm = cls._clamp_0_100(((road_intersection_density or 0.0) / 120.0) * 100.0) or 0.0
            mix_norm = cls._clamp_0_100(((lulc_mix_index or 0.0) / 3.0) * 100.0) or 0.0
            if road_circuity is None:
                circuity_score = 0.0
            elif road_circuity <= 1.0:
                circuity_score = 100.0
            else:
                circuity_score = 100.0 / (1.0 + ((road_circuity - 1.0) * 5.0))
            compactness = cls._clamp_0_100(
                (0.30 * (bcr or 0.0))
                + (0.25 * intersection_norm)
                + (0.25 * mix_norm)
                + (0.20 * circuity_score)
            )

        return {
            "cmp.walkability_index": walkability,
            "cmp.informality_index": informality,
            "cmp.heat_island_proxy": heat_island,
            "cmp.development_pressure": development_pressure,
            "cmp.topographic_constraint_expansion": topographic_constraint,
            "cmp.green_accessibility": green_accessibility,
            "cmp.transit_access_green": transit_access_green,
            "cmp.compactness": compactness,
        }

    def _augment_all_metrics(self, raw_metrics: Any) -> dict[str, Any]:
        metrics = dict(raw_metrics) if isinstance(raw_metrics, dict) else {}
        for metric_id in _RETIRED_METRIC_IDS:
            metrics.pop(metric_id, None)

        metrics.update(self._compute_composite_metrics(metrics))

        for metric_id in _RETIRED_METRIC_IDS:
            metrics.pop(metric_id, None)
        return metrics

    @staticmethod
    def _build_quality_summary(existing: Any, all_metrics: dict[str, Any]) -> dict[str, Any]:
        summary = dict(existing) if isinstance(existing, dict) else {}

        total_metrics = len(all_metrics)
        null_metrics = 0
        numeric_metrics = 0
        object_metrics = 0
        array_metrics = 0
        boolean_metrics = 0
        string_metrics = 0
        zero_metrics = 0

        for value in all_metrics.values():
            if value is None:
                null_metrics += 1
                continue
            if isinstance(value, bool):
                boolean_metrics += 1
                continue
            if isinstance(value, (int, float)):
                numeric = float(value)
                if math.isfinite(numeric):
                    numeric_metrics += 1
                    if abs(numeric) <= 1e-12:
                        zero_metrics += 1
                else:
                    string_metrics += 1
                continue
            if isinstance(value, dict):
                object_metrics += 1
                continue
            if isinstance(value, list):
                array_metrics += 1
                continue
            string_metrics += 1

        computed_non_null_metrics = total_metrics - null_metrics
        completeness_ratio = round(computed_non_null_metrics / total_metrics, 6) if total_metrics > 0 else None

        families_present = summary.get("families_present")
        if not isinstance(families_present, dict):
            families_present = {}

        prefix_set = {metric_id.split(".", 1)[0] for metric_id in all_metrics}
        families_present.update(
            {
                "roads": ("road" in prefix_set) or ("transit" in prefix_set),
                "buildings": "bldg" in prefix_set,
                "landuse": ("lulc" in prefix_set) or ("open" in prefix_set),
                "topography": "topo" in prefix_set,
                "composites": any(metric_id in all_metrics for metric_id in _COMPOSITE_METRIC_IDS),
            }
        )

        summary.update(
            {
                "total_metrics": total_metrics,
                "computed_non_null_metrics": computed_non_null_metrics,
                "null_metrics": null_metrics,
                "numeric_metrics": numeric_metrics,
                "object_metrics": object_metrics,
                "array_metrics": array_metrics,
                "boolean_metrics": boolean_metrics,
                "string_metrics": string_metrics,
                "zero_metrics": zero_metrics,
                "completeness_ratio": completeness_ratio,
                "families_present": families_present,
            }
        )
        return summary

    def _normalize_metrics_payload(
        self,
        metrics_json: Any,
        quality_summary: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        normalized_metrics_json = dict(metrics_json) if isinstance(metrics_json, dict) else {}
        all_metrics = self._augment_all_metrics(normalized_metrics_json.get("all_metrics"))
        normalized_metrics_json["all_metrics"] = all_metrics

        families = normalized_metrics_json.get("families")
        families_map = dict(families) if isinstance(families, dict) else {}
        families_map["composites"] = {key: all_metrics[key] for key in _COMPOSITE_METRIC_IDS if key in all_metrics}
        normalized_metrics_json["families"] = families_map
        normalized_metrics_json["metric_count"] = len(all_metrics)

        normalized_quality = self._build_quality_summary(quality_summary, all_metrics)
        return normalized_metrics_json, normalized_quality

    def _normalize_metric_row(self, row: dict[str, Any]) -> dict[str, Any]:
        metrics_json, quality_summary = self._normalize_metrics_payload(
            row.get("metrics_json"),
            row.get("quality_summary"),
        )
        row["metrics_json"] = metrics_json
        row["quality_summary"] = quality_summary
        return row

    async def _resolve_city_ward_table(self, city: str) -> str:
        normalized = self._normalize_city(city)
        table_name = f"{normalized}_wards_normalized"
        exists = await self._scalar(
            "city.table_exists",
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema='boundaries'
                AND table_name=:table_name
            )
            """,
            {"table_name": table_name},
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"City not found: {normalized}")
        return table_name

    async def _ensure_analysis_jobs_table(self) -> None:
        global _JOBS_TABLE_READY
        if _JOBS_TABLE_READY:
            return
        async with _JOBS_TABLE_LOCK:
            if _JOBS_TABLE_READY:
                return
            # Table and indexes are managed by alembic migration 0002_create_analysis_jobs.
            _JOBS_TABLE_READY = True

    async def _expire_stale_jobs(self, *, force: bool = False) -> None:
        global _LAST_STALE_EXPIRY_MONO
        now = time.monotonic()
        if not force and (now - _LAST_STALE_EXPIRY_MONO) < _STALE_EXPIRY_INTERVAL_SECONDS:
            return

        async with _STALE_EXPIRY_LOCK:
            now = time.monotonic()
            if not force and (now - _LAST_STALE_EXPIRY_MONO) < _STALE_EXPIRY_INTERVAL_SECONDS:
                return
            await self._execute(
                "jobs.expire_stale",
                """
                UPDATE meta.analysis_jobs
                SET
                  status='failed',
                  progress_pct=100,
                  progress_message='Timed out',
                  error_text=COALESCE(error_text, 'job_expired_before_completion'),
                  completed_at=COALESCE(completed_at, now()),
                  updated_at=now()
                WHERE status IN ('queued', 'running')
                  AND created_at < now() - make_interval(mins => :stale_minutes)
                """,
                {"stale_minutes": _JOB_STALE_QUEUE_MINUTES},
            )
            await self.session.commit()
            _LAST_STALE_EXPIRY_MONO = time.monotonic()

    @staticmethod
    def _job_from_row(row: dict[str, Any]) -> AnalyseJobResponse:
        return AnalyseJobResponse(
            job_id=row["job_id"],
            mode=row["mode"],
            city=row["city"],
            status=row["status"],
            progress_pct=int(row["progress_pct"]),
            progress_message=row.get("progress_message"),
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            result=row.get("result_json"),
            error=row.get("error_text"),
        )

    def spawn_job(self, job_id: str) -> None:
        task = asyncio.create_task(run_custom_polygon_job(job_id))
        _BACKGROUND_TASKS.add(task)
        task.add_done_callback(_cleanup_background_task)

    async def enqueue_custom_polygon_job(self, payload: AnalyseRequest) -> AnalyseJobResponse:
        if payload.mode != "custom_polygon":
            raise HTTPException(status_code=400, detail="Async jobs are only supported for custom_polygon mode")
        if payload.geometry is None:
            raise HTTPException(status_code=400, detail="geometry is required for custom_polygon mode")

        normalized = self._normalize_city(payload.city)
        await self._resolve_city_ward_table(normalized)
        await self._ensure_analysis_jobs_table()
        await self._expire_stale_jobs(force=True)

        job_id = str(uuid.uuid4())
        payload_json = json.dumps(
            {
                "city": normalized,
                "geometry": payload.geometry,
                "vintage_year": payload.vintage_year,
            }
        )

        row = (
            await self._execute(
                "jobs.enqueue",
                """
                INSERT INTO meta.analysis_jobs (
                  job_id,
                  mode,
                  city,
                  payload_json,
                  status,
                  progress_pct,
                  progress_message
                )
                VALUES (
                  :job_id,
                  'custom_polygon',
                  :city,
                  CAST(:payload_json AS jsonb),
                  'queued',
                  5,
                  'Queued'
                )
                RETURNING
                  job_id,
                  mode,
                  city,
                  status,
                  progress_pct,
                  progress_message,
                  created_at,
                  started_at,
                  completed_at,
                  result_json,
                  error_text
                """,
                {"job_id": job_id, "city": normalized, "payload_json": payload_json},
            )
        ).mappings().first()

        await self.session.commit()
        if row is None:
            raise HTTPException(status_code=500, detail="Failed to create analysis job")
        self.spawn_job(job_id)
        return self._job_from_row(dict(row))

    async def get_analyse_job(self, job_id: str) -> AnalyseJobResponse:
        await self._ensure_analysis_jobs_table()
        await self._expire_stale_jobs()
        row = (
            await self._execute(
                "jobs.get",
                """
                SELECT
                  job_id,
                  mode,
                  city,
                  status,
                  progress_pct,
                  progress_message,
                  created_at,
                  started_at,
                  completed_at,
                  result_json,
                  error_text
                FROM meta.analysis_jobs
                WHERE job_id=:job_id
                """,
                {"job_id": job_id},
            )
        ).mappings().first()

        if row is None:
            raise HTTPException(status_code=404, detail="Analysis job not found")

        row_dict = dict(row)
        if row_dict.get("status") == "queued":
            # If worker state was lost (server restart), polling can re-trigger execution.
            self.spawn_job(job_id)
        return self._job_from_row(row_dict)

    async def run_custom_polygon_job(self, job_id: str) -> None:
        await self._ensure_analysis_jobs_table()

        row = (
            await self._execute(
                "jobs.claim",
                """
                UPDATE meta.analysis_jobs
                SET
                  status='running',
                  progress_pct=20,
                  progress_message='Running analysis',
                  started_at=now(),
                  updated_at=now(),
                  error_text=NULL
                WHERE job_id=:job_id
                  AND (
                    status = 'queued'
                    OR (
                      status = 'running'
                      AND started_at < now() - make_interval(mins => :reclaim_after_minutes)
                    )
                  )
                RETURNING
                  job_id,
                  city,
                  payload_json,
                  status
                """,
                {"job_id": job_id, "reclaim_after_minutes": _JOB_RUNNING_RECLAIM_AFTER_MINUTES},
            )
        ).mappings().first()

        if row is None:
            # Job already completed, claimed by another worker, or not found.
            return

        await self.session.commit()

        payload_json = row["payload_json"]
        payload_obj = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
        city = str(row["city"])
        geometry = payload_obj.get("geometry") if isinstance(payload_obj, dict) else None
        vintage_year = payload_obj.get("vintage_year") if isinstance(payload_obj, dict) else None

        try:
            await self._execute(
                "jobs.progress_prepare",
                """
                UPDATE meta.analysis_jobs
                SET
                  progress_pct=35,
                  progress_message='Normalizing geometry and checking cache',
                  updated_at=now()
                WHERE job_id=:job_id
                  AND status='running'
                """,
                {"job_id": job_id},
            )
            await self.session.commit()

            await self._execute(
                "jobs.progress_compute",
                """
                UPDATE meta.analysis_jobs
                SET
                  progress_pct=65,
                  progress_message='Computing metrics (large polygons can take longer)',
                  updated_at=now()
                WHERE job_id=:job_id
                  AND status='running'
                """,
                {"job_id": job_id},
            )
            await self.session.commit()

            result = await self._analyse_custom_polygon(city, geometry, vintage_year)

            await self._execute(
                "jobs.mark_succeeded",
                """
                UPDATE meta.analysis_jobs
                SET
                  status='succeeded',
                  progress_pct=100,
                  progress_message='Complete',
                  result_json=CAST(:result_json AS jsonb),
                  completed_at=now(),
                  updated_at=now()
                WHERE job_id=:job_id
                  AND status='running'
                """,
                {"job_id": job_id, "result_json": json.dumps(result, default=str)},
            )
            await self.session.commit()
        except HTTPException as exc:
            OBS.record_metric_failure()
            await self.session.rollback()
            await self._execute(
                "jobs.mark_failed_http",
                """
                UPDATE meta.analysis_jobs
                SET
                  status='failed',
                  progress_pct=100,
                  progress_message='Failed',
                  error_text=:error_text,
                  completed_at=now(),
                  updated_at=now()
                WHERE job_id=:job_id
                  AND status='running'
                """,
                {"job_id": job_id, "error_text": f"{exc.status_code}: {exc.detail}"},
            )
            await self.session.commit()
        except Exception as exc:
            OBS.record_metric_failure()
            await self.session.rollback()
            await self._execute(
                "jobs.mark_failed_exception",
                """
                UPDATE meta.analysis_jobs
                SET
                  status='failed',
                  progress_pct=100,
                  progress_message='Failed',
                  error_text=:error_text,
                  completed_at=now(),
                  updated_at=now()
                WHERE job_id=:job_id
                  AND status='running'
                """,
                {"job_id": job_id, "error_text": f"{exc.__class__.__name__}: {exc}"},
            )
            await self.session.commit()
            logger.exception("analysis_job_execution_failed job_id=%s", job_id)

    async def list_cities(self) -> CitiesResponse:
        city_rows = (
            await self._execute(
                "cities.list",
                """
                SELECT
                  table_name,
                  regexp_replace(table_name, '_wards_normalized$', '') AS city
                FROM information_schema.tables
                WHERE table_schema='boundaries'
                  AND table_name LIKE '%\\_wards\\_normalized' ESCAPE '\\'
                  AND table_name NOT LIKE '%\\_source\\_%' ESCAPE '\\'
                ORDER BY city
                """,
            )
        ).mappings().all()

        if not city_rows:
            return CitiesResponse(cities=[])

        city_names = [row["city"] for row in city_rows]

        # Batch query: cached wards per city for current year
        cached_rows = (
            await self._execute(
                "cities.cached_wards_batch",
                """
                SELECT city, COUNT(*) AS cached_wards
                FROM metrics.ward_cache
                WHERE city = ANY(:cities)
                  AND vintage_year = EXTRACT(YEAR FROM CURRENT_DATE)::int
                GROUP BY city
                """,
                {"cities": city_names},
            )
        ).mappings().all()
        cached_by_city: dict[str, int] = {row["city"]: int(row["cached_wards"]) for row in cached_rows}

        # Batch query: expected wards per city (UNION of all ward tables)
        # Build a UNION ALL query dynamically — one COUNT per city table
        # table_name validated by information_schema LIKE filter above
        union_sql = " UNION ALL ".join(
            f"SELECT '{row['city']}'::text AS city, COUNT(*)::int AS expected_wards FROM boundaries.\"{row['table_name']}\""
            for row in city_rows
        )
        expected_rows = (
            await self._execute("cities.expected_wards_batch", union_sql)
        ).mappings().all()
        expected_by_city: dict[str, int] = {row["city"]: int(row["expected_wards"]) for row in expected_rows}

        cities: list[CitySummary] = []
        for row in city_rows:
            city = row["city"]
            expected = expected_by_city.get(city, 0)
            cached = cached_by_city.get(city, 0)
            pct = (cached / expected * 100.0) if expected > 0 else 0.0
            cities.append(
                CitySummary(
                    city=city,
                    expected_wards=expected,
                    cached_wards=cached,
                    completeness_pct=round(pct, 1),
                )
            )

        return CitiesResponse(cities=cities)

    async def list_city_wards(self, city: str) -> CityWardsResponse:
        normalized = self._normalize_city(city)
        table_name = await self._resolve_city_ward_table(normalized)

        rows = (
            await self._execute(
                "wards.list",
                f"""
                SELECT
                  b.ward_id::text AS ward_id,
                  b.ward_name::text AS ward_name,
                  b.ward_uid::text AS ward_uid,
                  (c.computed_at IS NOT NULL) AS has_cache,
                  c.computed_at
                FROM boundaries.{table_name} b
                LEFT JOIN LATERAL (
                  SELECT computed_at
                  FROM metrics.ward_cache c
                  WHERE c.city = :city
                    AND c.ward_id = b.ward_id::text
                  ORDER BY c.vintage_year DESC, c.computed_at DESC
                  LIMIT 1
                ) c ON TRUE
                ORDER BY b.ward_id::text
                """,
                {"city": normalized},
            )
        ).mappings().all()

        wards = [WardSummary(**dict(row)) for row in rows]
        cached = sum(1 for w in wards if w.has_cache)
        return CityWardsResponse(city=normalized, total_wards=len(wards), cached_wards=cached, wards=wards)

    async def list_city_wards_geojson(self, city: str) -> CityWardsGeoJSONResponse:
        normalized = self._normalize_city(city)
        table_name = await self._resolve_city_ward_table(normalized)

        rows = (
            await self._execute(
                "wards.geojson",
                f"""
                SELECT
                  b.ward_id::text AS ward_id,
                  b.ward_name::text AS ward_name,
                  b.ward_uid::text AS ward_uid,
                  ST_AsGeoJSON(ST_Transform(b.geom, 4326), 6)::jsonb AS geometry
                FROM boundaries.{table_name} b
                WHERE b.geom IS NOT NULL
                ORDER BY b.ward_id::text
                """,
            )
        ).mappings().all()

        features = [
            WardFeature(
                geometry=row["geometry"],
                properties=WardFeatureProperties(
                    ward_id=row["ward_id"],
                    ward_name=row["ward_name"],
                    ward_uid=row["ward_uid"],
                ),
            )
            for row in rows
        ]

        return CityWardsGeoJSONResponse(city=normalized, features=features)

    async def list_city_roads_geojson(
        self,
        city: str,
        bbox: str,
        zoom: float | None = None,
        detail: str | None = None,
    ) -> CityMapLayerGeoJSONResponse:
        normalized = self._normalize_city(city)
        west, south, east, north = self._parse_bbox(bbox)
        detail_mode = self._road_detail(detail, zoom)
        tolerance_m = self._road_simplify_tolerance_m(zoom)
        table_name = f"{normalized}_roads_normalized"

        exists = await self._scalar(
            "roads.layer.table_exists",
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema='transport'
                AND table_name=:table_name
            )
            """,
            {"table_name": table_name},
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"Road layer not found for city: {normalized}")

        if detail_mode == "major":
            highway_filter = """
              AND lower(COALESCE(r.highway, '')) IN (
                'motorway', 'motorway_link',
                'trunk', 'trunk_link',
                'primary', 'primary_link',
                'secondary', 'secondary_link',
                'tertiary', 'tertiary_link'
              )
            """
            limit = 6000
        else:
            highway_filter = """
              AND lower(COALESCE(r.highway, '')) NOT IN (
                'footway', 'path', 'steps', 'track', 'cycleway', 'crossing'
              )
            """
            limit = 12000

        rows = (
            await self._execute(
                "roads.layer.geojson",
                f"""
                WITH bounds AS (
                  SELECT ST_MakeEnvelope(:west, :south, :east, :north, 4326) AS geom
                ),
                src AS (
                  SELECT
                    COALESCE(NULLIF(lower(r.highway), ''), 'unknown') AS road_class,
                    CASE
                      WHEN lower(COALESCE(r.highway, '')) IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') THEN 4
                      WHEN lower(COALESCE(r.highway, '')) IN ('primary', 'primary_link') THEN 3
                      WHEN lower(COALESCE(r.highway, '')) IN ('secondary', 'secondary_link') THEN 2
                      ELSE 1
                    END AS style_rank,
                    ST_AsGeoJSON(
                      ST_Transform(
                        ST_SimplifyPreserveTopology(
                          ST_Transform(
                            ST_CollectionExtract(ST_MakeValid(ST_Intersection(r.geom, b.geom)), 2),
                            3857
                          ),
                          :tolerance_m
                        ),
                        4326
                      ),
                      5
                    )::jsonb AS geometry
                  FROM transport.{table_name} r
                  CROSS JOIN bounds b
                  WHERE r.geom IS NOT NULL
                    AND NOT ST_IsEmpty(r.geom)
                    AND r.geom && b.geom
                    AND ST_Intersects(r.geom, b.geom)
                    {highway_filter}
                )
                SELECT road_class, style_rank, geometry
                FROM src
                WHERE geometry IS NOT NULL
                ORDER BY style_rank DESC, road_class
                LIMIT :limit
                """,
                {
                    "west": west,
                    "south": south,
                    "east": east,
                    "north": north,
                    "tolerance_m": tolerance_m,
                    "limit": limit,
                },
            )
        ).mappings().all()

        features = [
            MapLayerFeature(
                geometry=row["geometry"],
                properties={
                    "road_class": row["road_class"],
                    "style_rank": int(row["style_rank"]),
                },
            )
            for row in rows
        ]
        return CityMapLayerGeoJSONResponse(
            city=normalized,
            layer="roads",
            feature_count=len(features),
            features=features,
        )

    async def list_city_transit_geojson(
        self,
        city: str,
        bbox: str,
    ) -> CityMapLayerGeoJSONResponse:
        normalized = self._normalize_city(city)
        west, south, east, north = self._parse_bbox(bbox)
        table_name = f"{normalized}_transit_normalized"

        exists = await self._scalar(
            "transit.layer.table_exists",
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema='transport'
                AND table_name=:table_name
            )
            """,
            {"table_name": table_name},
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"Transit layer not found for city: {normalized}")

        rows = (
            await self._execute(
                "transit.layer.geojson",
                f"""
                WITH bounds AS (
                  SELECT ST_MakeEnvelope(:west, :south, :east, :north, 4326) AS geom
                ),
                src AS (
                  SELECT
                    t.source_layer,
                    CASE
                      WHEN t.source_layer IN ('metro_stations', 'metro_entrances') THEN 'metro'
                      WHEN t.source_layer = 'rail_stations' THEN 'rail'
                      WHEN t.source_layer = 'public_transport_stations' THEN 'station'
                      ELSE 'stop'
                    END AS stop_kind,
                    ST_AsGeoJSON(
                      ST_Transform(
                        ST_PointOnSurface(t.geom),
                        4326
                      ),
                      6
                    )::jsonb AS geometry
                  FROM transport.{table_name} t
                  CROSS JOIN bounds b
                  WHERE t.geom IS NOT NULL
                    AND NOT ST_IsEmpty(t.geom)
                    AND t.geom && b.geom
                    AND ST_Intersects(t.geom, b.geom)
                    AND t.source_layer IN ({_TRANSIT_POINT_LAYER_SQL})
                )
                SELECT source_layer, stop_kind, geometry
                FROM src
                WHERE geometry IS NOT NULL
                ORDER BY
                  CASE stop_kind
                    WHEN 'metro' THEN 1
                    WHEN 'rail' THEN 2
                    WHEN 'station' THEN 3
                    ELSE 4
                  END,
                  source_layer
                LIMIT 5000
                """,
                {
                    "west": west,
                    "south": south,
                    "east": east,
                    "north": north,
                },
            )
        ).mappings().all()

        features = [
            MapLayerFeature(
                geometry=row["geometry"],
                properties={
                    "source_layer": row["source_layer"],
                    "stop_kind": row["stop_kind"],
                },
            )
            for row in rows
        ]
        return CityMapLayerGeoJSONResponse(
            city=normalized,
            layer="transit",
            feature_count=len(features),
            features=features,
        )

    async def get_ward_metrics(self, city: str, ward_id: str) -> WardMetricResponse:
        normalized = self._normalize_city(city)
        row = (
            await self._execute(
                "ward.metrics",
                """
                SELECT
                  city,
                  ward_id,
                  ward_uid,
                  ward_name,
                  vintage_year,
                  metrics_json,
                  quality_summary,
                  computed_at
                FROM metrics.ward_cache
                WHERE city=:city
                  AND ward_id=:ward_id
                ORDER BY vintage_year DESC, computed_at DESC
                LIMIT 1
                """,
                {"city": normalized, "ward_id": ward_id.strip()},
            )
        ).mappings().first()

        if row is None:
            raise HTTPException(status_code=404, detail="Ward metrics not found")

        normalized_row = self._normalize_metric_row(dict(row))
        return WardMetricResponse(**normalized_row)

    async def get_city_metrics(self, city: str) -> CityMetricsResponse:
        normalized = self._normalize_city(city)
        rows = (
            await self._execute(
                "city.metrics.latest_rows",
                """
                WITH latest AS (
                  SELECT DISTINCT ON (ward_id)
                    ward_id,
                    metrics_json
                  FROM metrics.ward_cache
                  WHERE city=:city
                  ORDER BY ward_id, vintage_year DESC, computed_at DESC
                )
                SELECT ward_id, metrics_json
                FROM latest
                ORDER BY ward_id
                """,
                {"city": normalized},
            )
        ).mappings().all()

        ward_count = len(rows)
        if ward_count == 0:
            raise HTTPException(status_code=404, detail=f"No cached ward metrics for city: {normalized}")

        stats: dict[str, dict[str, float | int]] = defaultdict(lambda: {"sum": 0.0, "min": math.inf, "max": -math.inf, "count": 0})

        for row in rows:
            metrics_json, _ = self._normalize_metrics_payload(row.get("metrics_json"), None)
            all_metrics = metrics_json.get("all_metrics")
            if not isinstance(all_metrics, dict):
                continue

            for metric_id, raw_value in all_metrics.items():
                numeric = self._metric_number(raw_value)
                if numeric is None:
                    continue

                metric_stats = stats[metric_id]
                metric_stats["sum"] = float(metric_stats["sum"]) + numeric
                metric_stats["min"] = min(float(metric_stats["min"]), numeric)
                metric_stats["max"] = max(float(metric_stats["max"]), numeric)
                metric_stats["count"] = int(metric_stats["count"]) + 1

        metrics: list[MetricAggregate] = []
        for metric_id in sorted(stats):
            metric_stats = stats[metric_id]
            sample_count = int(metric_stats["count"])
            if sample_count <= 0:
                continue
            metrics.append(
                MetricAggregate(
                    metric_id=metric_id,
                    avg_value=float(metric_stats["sum"]) / sample_count,
                    min_value=float(metric_stats["min"]),
                    max_value=float(metric_stats["max"]),
                    sample_count=sample_count,
                )
            )

        return CityMetricsResponse(city=normalized, ward_count=ward_count, metric_count=len(metrics), metrics=metrics)

    async def get_meta_metrics(self) -> MetaMetricsResponse:
        rows = (
            await self._execute(
                "meta.metrics.db",
                """
                SELECT
                  metric_id,
                  label,
                  category,
                  unit,
                  frontend_group,
                  status,
                  release_target,
                  backend_function,
                  source_layers,
                  formula_summary
                FROM meta.metric_registry
                ORDER BY metric_id
                """,
            )
        ).mappings().all()

        if rows:
            metrics = [
                MetricMetaItem(**dict(row))
                for row in rows
                if dict(row).get("metric_id") not in _RETIRED_METRIC_IDS
            ]
            for metric in metrics:
                if metric.metric_id == "cmp.topographic_constraint_expansion":
                    metric.formula_summary = "Composite score combining natural-constraint index and steep-slope share."
                    metric.source_layers = ["slope_raster", "water_bodies"]
            return MetaMetricsResponse(source="meta.metric_registry", count=len(metrics), metrics=metrics)

        registry_path = Path(__file__).resolve().parents[3] / "metrics_registry.yaml"
        if not registry_path.exists():
            raise HTTPException(status_code=404, detail="Metric registry not found")

        content = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
        items = content.get("metrics", []) if isinstance(content, dict) else []
        metrics = [
            MetricMetaItem(
                metric_id=i.get("metric_id", ""),
                label=i.get("label", ""),
                category=i.get("category"),
                unit=i.get("unit"),
                frontend_group=i.get("frontend_group"),
                status=i.get("status"),
                release_target=i.get("release_target"),
                backend_function=i.get("backend_function"),
                source_layers=i.get("source_layers"),
                formula_summary=i.get("formula_summary"),
            )
            for i in items
            if i.get("metric_id", "") not in _RETIRED_METRIC_IDS
        ]
        for metric in metrics:
            if metric.metric_id == "cmp.topographic_constraint_expansion":
                metric.formula_summary = "Composite score combining natural-constraint index and steep-slope share."
                metric.source_layers = ["slope_raster", "water_bodies"]
        return MetaMetricsResponse(source="metrics_registry.yaml", count=len(metrics), metrics=metrics)

    async def analyse(self, payload: AnalyseRequest) -> AnalyseResponse:
        start = time.perf_counter()
        try:
            if payload.mode == "ward":
                result = (await self.get_ward_metrics(payload.city, payload.ward_id or "")).model_dump(mode="json")
            elif payload.mode == "wards":
                result = await self._analyse_wards(payload.city, payload.ward_ids, payload.limit)
            elif payload.mode == "city":
                result = await self._analyse_city_full(payload.city)
            else:
                result = await self._analyse_custom_polygon(payload.city, payload.geometry, payload.vintage_year)
                await self.session.commit()
        except HTTPException:
            OBS.record_metric_failure()
            raise
        except SQLAlchemyError as exc:
            OBS.record_metric_failure()
            logger.exception("analyse_sqlalchemy_error mode=%s", payload.mode)
            raise HTTPException(status_code=503, detail=f"Database error: {exc.__class__.__name__}") from exc

        duration_ms = (time.perf_counter() - start) * 1000.0
        return AnalyseResponse(mode=payload.mode, city=payload.city, result=result, timing_ms=round(duration_ms, 3))

    async def _analyse_wards(self, city: str, ward_ids: list[str] | None, limit: int) -> dict[str, Any]:
        normalized = self._normalize_city(city)
        where_clause = "city=:city"
        params: dict[str, Any] = {"city": normalized, "limit": min(max(limit, 1), 1000)}

        if ward_ids:
            cleaned = [w.strip() for w in ward_ids if w and w.strip()]
            if not cleaned:
                raise HTTPException(status_code=400, detail="ward_ids provided but empty after cleanup")
            params["ward_ids"] = cleaned
            where_clause += " AND ward_id = ANY(:ward_ids)"

        rows = (
            await self._execute(
                "analyse.wards",
                f"""
                WITH latest AS (
                  SELECT DISTINCT ON (ward_id)
                    city,
                    ward_id,
                    ward_uid,
                    ward_name,
                    vintage_year,
                    metrics_json,
                    quality_summary,
                    computed_at
                  FROM metrics.ward_cache
                  WHERE {where_clause}
                  ORDER BY ward_id, vintage_year DESC, computed_at DESC
                )
                SELECT
                  city,
                  ward_id,
                  ward_uid,
                  ward_name,
                  vintage_year,
                  metrics_json,
                  quality_summary,
                  computed_at
                FROM latest
                ORDER BY ward_id
                LIMIT :limit
                """,
                params,
            )
        ).mappings().all()

        items = [WardMetricResponse(**self._normalize_metric_row(dict(row))).model_dump(mode="json") for row in rows]
        return {
            "city": normalized,
            "count": len(items),
            "wards": items,
        }

    async def _analyse_city_full(self, city: str) -> dict[str, Any]:
        normalized = self._normalize_city(city)
        city_aggregate = await self.get_city_metrics(normalized)
        all_metrics = {item.metric_id: item.avg_value for item in city_aggregate.metrics}
        metrics_json, quality_summary = self._normalize_metrics_payload(
            {
                "city": normalized,
                "input": {"aggregation": "latest_ward_average"},
                "all_metrics": all_metrics,
                "metric_count": len(all_metrics),
            },
            None,
        )

        return {
            "city": normalized,
            "scope": "city_ward_average",
            "ward_count": city_aggregate.ward_count,
            "metrics_json": metrics_json,
            "quality_summary": quality_summary,
        }

    async def _analyse_custom_polygon(
        self,
        city: str,
        geometry: dict[str, Any] | None,
        vintage_year: int | None,
    ) -> dict[str, Any]:
        if geometry is None:
            raise HTTPException(status_code=400, detail="geometry is required for custom_polygon mode")

        normalized = self._normalize_city(city)

        geom_json = json.dumps(geometry)
        params: dict[str, Any] = {
            "city": normalized,
            "geom_json": geom_json,
            "vintage_year": vintage_year,
        }

        await self._execute(
            "analyse.custom_polygon.statement_timeout",
            f"SET LOCAL statement_timeout = '{_CUSTOM_ANALYSIS_STATEMENT_TIMEOUT}'",
        )

        row = (
            await self._execute(
                "analyse.custom_polygon",
                """
                SELECT
                  city,
                  geom_hash,
                  vintage_year,
                  cache_hit,
                  metrics_json,
                  quality_summary,
                  computed_at
                FROM metrics.get_or_compute_custom_cache(
                  :city,
                  ST_SetSRID(ST_GeomFromGeoJSON(:geom_json), 4326),
                  COALESCE(:vintage_year, EXTRACT(YEAR FROM CURRENT_DATE)::int)
                )
                """,
                params,
            )
        ).mappings().first()

        if row is None:
            raise HTTPException(status_code=500, detail="Custom analysis produced no row")

        OBS.record_custom_cache(bool(row["cache_hit"]))
        normalized_row = self._normalize_metric_row(dict(row))
        return normalized_row

    async def get_health(self) -> HealthResponse:
        checks = {"database": "ok"}
        status = "ok"
        try:
            await self._execute("health.db", "SELECT 1")
        except Exception:
            checks["database"] = "error"
            status = "degraded"

        return HealthResponse(status=status, checks=checks, observability=OBS.snapshot())
