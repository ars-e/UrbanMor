from typing import Any, Literal

from pydantic import BaseModel


class MapLayerFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: dict[str, Any]
    properties: dict[str, Any]


class CityMapLayerGeoJSONResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    city: str
    layer: Literal["roads", "transit"]
    feature_count: int
    features: list[MapLayerFeature]
