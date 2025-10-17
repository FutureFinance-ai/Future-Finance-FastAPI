from typing import Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# from services.config import IS_DEV, CORS_ALLOW_ORIGINS, CORS_ALLOW_CREDENTIALS
from auth.routes import router as auth_router
from upload_service.upload_route import router as upload_router
from auth.db import init_db, close_db
import logging

logger = logging.getLogger(__name__)


def get_app() -> FastAPI:
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
