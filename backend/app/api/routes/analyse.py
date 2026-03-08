from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from app.api.deps import get_analyse_service
from app.schemas.analysis import AnalyseJobResponse, AnalyseRequest, AnalyseResponse
from app.services.analyse import AnalyseService

router = APIRouter(tags=["analyse"])


@router.post(
    "/analyse",
    response_model=AnalyseResponse | AnalyseJobResponse,
    status_code=status.HTTP_200_OK,
)
async def analyse(
    payload: AnalyseRequest,
    response: Response,
    service: Annotated[AnalyseService, Depends(get_analyse_service)],
) -> AnalyseResponse | AnalyseJobResponse:
    if payload.mode == "custom_polygon" and payload.run_async is True:
        job = await service.enqueue_custom_polygon_job(payload)
        service.spawn_job(job.job_id)
        response.status_code = status.HTTP_202_ACCEPTED
        return job
    return await service.analyse(payload)


@router.get("/analyse/jobs/{job_id}", response_model=AnalyseJobResponse)
async def get_analyse_job(
    job_id: str,
    service: Annotated[AnalyseService, Depends(get_analyse_service)],
) -> AnalyseJobResponse:
    return await service.get_analyse_job(job_id)
