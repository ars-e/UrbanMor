from enum import Enum
from typing import Any
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class AnalysisMode(str, Enum):
    ward = "ward"
    wards = "wards"
    city = "city"
    custom_polygon = "custom_polygon"


class AnalyseRequest(BaseModel):
    mode: AnalysisMode
    city: str
    ward_id: str | None = None
    ward_ids: list[str] | None = None
    geometry: dict[str, Any] | None = None
    vintage_year: int | None = Field(default=None, ge=1900, le=2100)
    limit: int = Field(default=200, ge=1, le=1000)
    run_async: bool | None = None

    @model_validator(mode="after")
    def _validate_mode_requirements(self):
        if self.mode == AnalysisMode.ward and not self.ward_id:
            raise ValueError("ward_id is required when mode=ward")
        if self.mode == AnalysisMode.custom_polygon and self.geometry is None:
            raise ValueError("geometry is required when mode=custom_polygon")
        return self


class AnalyseResponse(BaseModel):
    mode: AnalysisMode
    city: str
    result: dict[str, Any]
    timing_ms: float


class AnalyseJobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class AnalyseJobResponse(BaseModel):
    job_id: str
    mode: AnalysisMode
    city: str
    status: AnalyseJobStatus
    progress_pct: int
    progress_message: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
