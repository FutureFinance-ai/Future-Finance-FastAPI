from typing import Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# from services.config import IS_DEV, CORS_ALLOW_ORIGINS, CORS_ALLOW_CREDENTIALS
from auth.routes import router as auth_router
from upload_service.upload_route import router as upload_router
from analysis_service.analysis_routes import router as analysis_router
from auth.db import init_db, close_db
import logging
from settings.config import settings
from budgets.budget_routes import router as budget_router
from alerts.alerts_routes import router as alerts_router
from storage.routes import router as storage_router
from jobs.routes import router as jobs_router
from ai.routes import router as ai_router
from settings.logging_config import configure_logging
from auth.workspace_routes import router as workspace_router

logger = logging.getLogger(__name__)


def get_app() -> FastAPI:
    configure_logging()
    logger.info("Starting FutureFinance API")
    app = FastAPI(title="FutureFinance API")

    # CORS: enable permissive defaults for local development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # DB lifecycle
    @app.on_event("startup")
    async def on_startup() -> None:
        logger.info("Initializing database")
        await init_db()

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        logger.info("Closing database")
        await close_db()

    # Routers
    app.include_router(auth_router)
    app.include_router(upload_router)
    app.include_router(analysis_router)
    app.include_router(alerts_router)
    app.include_router(storage_router)
    app.include_router(jobs_router)
    app.include_router(ai_router)
    app.include_router(workspace_router)
    if settings.ENABLE_BUDGETS:
        app.include_router(budget_router)
    logger.info("Routers initialized successfully")

    # Health
    @app.get("/health")
    async def health_check() -> Dict[str, str]:
        logger.info("Health check")
        return {"status": "ok"}

    # Upload endpoint
    
    logger.info("API started")
    return app


# ASGI app instance
app = get_app()
