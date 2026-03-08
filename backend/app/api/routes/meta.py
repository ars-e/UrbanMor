from fastapi import APIRouter, Depends

from app.api.deps import get_analyse_service
from app.schemas.meta import MetaMetricsResponse
from app.services.analyse import AnalyseService

router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("/metrics", response_model=MetaMetricsResponse)
async def metric_meta(service: AnalyseService = Depends(get_analyse_service)) -> MetaMetricsResponse:
    return await service.get_meta_metrics()
