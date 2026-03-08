from typing import Any, Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, str]
    observability: dict[str, Any]
