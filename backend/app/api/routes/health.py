from fastapi import APIRouter, Depends

from app.api.deps import get_analyse_service
from app.schemas.health import HealthResponse
from app.services.analyse import AnalyseService

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(service: AnalyseService = Depends(get_analyse_service)) -> HealthResponse:
    return await service.get_health()
