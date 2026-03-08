import logging
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.observability import OBS, configure_logging

settings = get_settings()
configure_logging()
logger = logging.getLogger("urbanmor.api")

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=settings.cors_origins_list != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000.0

    OBS.record_request(duration_ms)
    response.headers["X-Request-Duration-Ms"] = f"{duration_ms:.3f}"
    logger.info(
        "request method=%s path=%s status=%s duration_ms=%.3f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {"service": settings.app_name, "status": "ok"}


app.include_router(api_router)
