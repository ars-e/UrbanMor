from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.schemas.metrics import WardMetricResponse

router = APIRouter(prefix="/v1/metrics", tags=["metrics"])


@router.get(
    "/wards/{city}/{ward_id}",
    response_model=WardMetricResponse,
    summary="Get cached metrics for one ward",
)
async def ward_metrics(
    city: str = Path(..., min_length=1, max_length=64),
    ward_id: str = Path(..., min_length=1, max_length=128),
    session: AsyncSession = Depends(get_db_session),
) -> WardMetricResponse:
    sql = text(
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
        FROM metrics.ward_cache_latest
        WHERE city = :city
          AND ward_id = :ward_id
        """
    )

    try:
        result = await session.execute(sql, {"city": city.lower().strip(), "ward_id": ward_id.strip()})
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc.__class__.__name__}") from exc

    row = result.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Ward metrics not found")

    payload: dict[str, Any] = dict(row)
    return WardMetricResponse(**payload)
