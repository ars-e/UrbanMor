from fastapi import APIRouter, Depends

from app.api.deps import get_analyse_service
from app.schemas.cities import CitiesResponse, CityMetricsResponse, CityWardsGeoJSONResponse, CityWardsResponse
from app.schemas.metrics import WardMetricResponse
from app.services.analyse import AnalyseService

router = APIRouter(tags=["cities"])


@router.get("/cities", response_model=CitiesResponse)
async def list_cities(service: AnalyseService = Depends(get_analyse_service)) -> CitiesResponse:
    return await service.list_cities()


@router.get("/cities/{city}/wards", response_model=CityWardsResponse)
async def list_wards(city: str, service: AnalyseService = Depends(get_analyse_service)) -> CityWardsResponse:
    return await service.list_city_wards(city)


@router.get("/cities/{city}/wards/geojson", response_model=CityWardsGeoJSONResponse)
async def list_ward_geometries(
    city: str,
    service: AnalyseService = Depends(get_analyse_service),
) -> CityWardsGeoJSONResponse:
    return await service.list_city_wards_geojson(city)


@router.get("/cities/{city}/metrics", response_model=CityMetricsResponse)
async def city_metrics(city: str, service: AnalyseService = Depends(get_analyse_service)) -> CityMetricsResponse:
    return await service.get_city_metrics(city)


@router.get("/cities/{city}/wards/{ward_id}", response_model=WardMetricResponse)
async def ward_metrics(city: str, ward_id: str, service: AnalyseService = Depends(get_analyse_service)) -> WardMetricResponse:
    return await service.get_ward_metrics(city, ward_id)
