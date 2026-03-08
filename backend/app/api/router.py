from fastapi import APIRouter

from app.api.routes.analyse import router as analyse_router
from app.api.routes.cities import router as cities_router
from app.api.routes.health import router as health_router
from app.api.routes.meta import router as meta_router
from app.api.routes.metrics import router as metrics_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(cities_router)
api_router.include_router(analyse_router)
api_router.include_router(meta_router)
api_router.include_router(metrics_router)
