from datetime import datetime
from typing import Any

from pydantic import BaseModel


class WardMetricResponse(BaseModel):
    city: str
    ward_id: str
    ward_uid: str | None = None
    ward_name: str | None = None
    vintage_year: int
    metrics_json: dict[str, Any]
    quality_summary: dict[str, Any]
    computed_at: datetime
