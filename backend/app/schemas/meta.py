from pydantic import BaseModel


class MetricMetaItem(BaseModel):
    metric_id: str
    label: str
    category: str | None = None
    unit: str | None = None
    frontend_group: str | None = None
    status: str | None = None
    release_target: str | None = None
    backend_function: str | None = None
    source_layers: list[str] | None = None
    formula_summary: str | None = None


class MetaMetricsResponse(BaseModel):
    source: str
    count: int
    metrics: list[MetricMetaItem]
