from typing import Dict

from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from schemas.UploadData import DocumentUploadResponse
from services.statement_service import StatementService
# from services.config import IS_DEV, CORS_ALLOW_ORIGINS, CORS_ALLOW_CREDENTIALS
from auth.routes import router as auth_router
from auth.db import init_db, close_db


statement_service = StatementService()


def get_app() -> FastAPI:
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
        await init_db()

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await close_db()

    # Routers
    app.include_router(auth_router)

    # Health
    @app.get("/health")
    async def health_check() -> Dict[str, str]:
        return {"status": "ok"}

    # Upload endpoint
    @app.post("/upload-bank-statement", response_model=DocumentUploadResponse)
    async def upload_bank_statement(
        file: UploadFile = File(...),
    ) -> DocumentUploadResponse:
        try:
            return await statement_service.upload_pdf_bank_statement(file)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e),
            )

    return app


# ASGI app instance
app = get_app()
