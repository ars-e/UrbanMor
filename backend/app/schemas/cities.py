from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class CitySummary(BaseModel):
    city: str
    expected_wards: int
    cached_wards: int
    completeness_pct: float


class CitiesResponse(BaseModel):
    cities: list[CitySummary]


class WardSummary(BaseModel):
    ward_id: str
    ward_name: str | None = None
    ward_uid: str | None = None
    has_cache: bool
    computed_at: datetime | None = None


class CityWardsResponse(BaseModel):
    city: str
    total_wards: int
    cached_wards: int
    wards: list[WardSummary]


class MetricAggregate(BaseModel):
    metric_id: str
    avg_value: float
    min_value: float
    max_value: float
    sample_count: int


class CityMetricsResponse(BaseModel):
    city: str
    ward_count: int
    metric_count: int
    metrics: list[MetricAggregate]


class WardFeatureProperties(BaseModel):
    ward_id: str
    ward_name: str | None = None
    ward_uid: str | None = None


class WardFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: dict[str, Any]
    properties: WardFeatureProperties


class CityWardsGeoJSONResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    city: str
    features: list[WardFeature]
