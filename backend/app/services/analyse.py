import asyncio
import json
import logging
import re
import time
import uuid
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
from app.schemas.meta import MetaMetricsResponse, MetricMetaItem
from app.schemas.metrics import WardMetricResponse

logger = logging.getLogger("urbanmor.service.analyse")
_CITY_RE = re.compile(r"^[a-z0-9_]+$")
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()
_JOB_SEMAPHORE = asyncio.Semaphore(2)
_JOBS_TABLE_READY = False
_JOBS_TABLE_LOCK = asyncio.Lock()


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

            await self._execute("jobs.schema", "CREATE SCHEMA IF NOT EXISTS meta")
            await self._execute(
                "jobs.table",
                """
                CREATE TABLE IF NOT EXISTS meta.analysis_jobs (
                  job_id text PRIMARY KEY,
                  mode text NOT NULL,
                  city text NOT NULL,
                  payload_json jsonb NOT NULL,
                  status text NOT NULL,
                  progress_pct integer NOT NULL DEFAULT 0,
                  progress_message text,
                  result_json jsonb,
                  error_text text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  started_at timestamptz,
                  completed_at timestamptz,
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """,
            )
            await self._execute(
                "jobs.index_status",
                """
                CREATE INDEX IF NOT EXISTS analysis_jobs_status_created_idx
                ON meta.analysis_jobs(status, created_at DESC)
                """,
            )
            await self._execute(
                "jobs.index_city",
                """
                CREATE INDEX IF NOT EXISTS analysis_jobs_city_created_idx
                ON meta.analysis_jobs(city, created_at DESC)
                """,
            )
            await self.session.commit()
            _JOBS_TABLE_READY = True

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
        return self._job_from_row(dict(row))

    async def get_analyse_job(self, job_id: str) -> AnalyseJobResponse:
        await self._ensure_analysis_jobs_table()
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
        return self._job_from_row(dict(row))

    async def run_custom_polygon_job(self, job_id: str) -> None:
        await self._ensure_analysis_jobs_table()

        row = (
            await self._execute(
                "jobs.get_payload",
                """
                SELECT
                  job_id,
                  city,
                  payload_json,
                  status
                FROM meta.analysis_jobs
                WHERE job_id=:job_id
                """,
                {"job_id": job_id},
            )
        ).mappings().first()

        if row is None:
            logger.error("analysis_job_not_found job_id=%s", job_id)
            return

        status_value = str(row["status"])
        if status_value in {"succeeded", "failed"}:
            return

        await self._execute(
            "jobs.mark_running",
            """
            UPDATE meta.analysis_jobs
            SET
              status='running',
              progress_pct=20,
              progress_message='Running analysis',
              started_at=COALESCE(started_at, now()),
              updated_at=now(),
              error_text=NULL
            WHERE job_id=:job_id
            """,
            {"job_id": job_id},
        )
        await self.session.commit()

        payload_json = row["payload_json"]
        payload_obj = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
        city = str(row["city"])
        geometry = payload_obj.get("geometry") if isinstance(payload_obj, dict) else None
        vintage_year = payload_obj.get("vintage_year") if isinstance(payload_obj, dict) else None

        try:
            await self._execute(
                "jobs.progress_compute",
                """
                UPDATE meta.analysis_jobs
                SET
                  progress_pct=60,
                  progress_message='Computing metric payload',
                  updated_at=now()
                WHERE job_id=:job_id
                """,
                {"job_id": job_id},
            )
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
                """,
                {"job_id": job_id, "result_json": json.dumps(result, default=str)},
            )
            await self.session.commit()
        except HTTPException as exc:
            OBS.record_metric_failure()
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
                """,
                {"job_id": job_id, "error_text": f"{exc.status_code}: {exc.detail}"},
            )
            await self.session.commit()
        except Exception as exc:
            OBS.record_metric_failure()
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

        cities: list[CitySummary] = []
        for row in city_rows:
            city = row["city"]
            table_name = row["table_name"]

            expected_wards = await self._scalar(
                "cities.expected_wards",
                f"SELECT COUNT(*) FROM boundaries.{table_name}",
            )
            cached_wards = await self._scalar(
                "cities.cached_wards",
                """
                SELECT COUNT(*)
                FROM metrics.ward_cache
                WHERE city=:city
                  AND vintage_year=EXTRACT(YEAR FROM CURRENT_DATE)::int
                """,
                {"city": city},
            )
            expected_wards = int(expected_wards or 0)
            cached_wards = int(cached_wards or 0)
            pct = (cached_wards / expected_wards * 100.0) if expected_wards > 0 else 0.0

            cities.append(
                CitySummary(
                    city=city,
                    expected_wards=expected_wards,
                    cached_wards=cached_wards,
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

        return WardMetricResponse(**dict(row))

    async def get_city_metrics(self, city: str) -> CityMetricsResponse:
        normalized = self._normalize_city(city)

        ward_count = await self._scalar(
            "city.metrics.ward_count",
            """
            SELECT COUNT(DISTINCT ward_id)
            FROM metrics.ward_cache
            WHERE city=:city
            """,
            {"city": normalized},
        )
        ward_count = int(ward_count or 0)
        if ward_count == 0:
            raise HTTPException(status_code=404, detail=f"No cached ward metrics for city: {normalized}")

        rows = (
            await self._execute(
                "city.metrics.aggregate",
                """
                WITH latest AS (
                  SELECT DISTINCT ON (ward_id)
                    ward_id,
                    metrics_json
                  FROM metrics.ward_cache
                  WHERE city=:city
                  ORDER BY ward_id, vintage_year DESC, computed_at DESC
                ),
                src AS (
                  SELECT metrics_json->'all_metrics' AS all_metrics
                  FROM latest
                ),
                flat AS (
                  SELECT
                    kv.key AS metric_id,
                    (kv.value::text)::double precision AS value
                  FROM src
                  CROSS JOIN LATERAL jsonb_each(src.all_metrics) kv
                  WHERE jsonb_typeof(kv.value)='number'
                )
                SELECT
                  metric_id,
                  AVG(value)::double precision AS avg_value,
                  MIN(value)::double precision AS min_value,
                  MAX(value)::double precision AS max_value,
                  COUNT(*)::int AS sample_count
                FROM flat
                GROUP BY metric_id
                ORDER BY metric_id
                """,
                {"city": normalized},
            )
        ).mappings().all()

        metrics = [MetricAggregate(**dict(row)) for row in rows]
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
            metrics = [MetricMetaItem(**dict(row)) for row in rows]
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
        ]
        return MetaMetricsResponse(source="metrics_registry.yaml", count=len(metrics), metrics=metrics)

    async def analyse(self, payload: AnalyseRequest) -> AnalyseResponse:
        start = time.perf_counter()
        try:
            if payload.mode == "ward":
                result = (await self.get_ward_metrics(payload.city, payload.ward_id or "")).model_dump(mode="json")
            elif payload.mode == "wards":
                result = await self._analyse_wards(payload.city, payload.ward_ids, payload.limit)
            elif payload.mode == "city":
                result = (await self.get_city_metrics(payload.city)).model_dump(mode="json")
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

        items = [WardMetricResponse(**dict(row)).model_dump(mode="json") for row in rows]
        return {
            "city": normalized,
            "count": len(items),
            "wards": items,
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
        return dict(row)

    async def get_health(self) -> HealthResponse:
        checks = {"database": "ok"}
        status = "ok"
        try:
            await self._execute("health.db", "SELECT 1")
        except Exception:
            checks["database"] = "error"
            status = "degraded"

        return HealthResponse(status=status, checks=checks, observability=OBS.snapshot())
