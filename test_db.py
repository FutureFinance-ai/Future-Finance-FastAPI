from __future__ import annotations

from typing import Optional

from fastapi import Depends
from pydantic import BaseModel, Field
from surrealdb import AsyncSurreal
from services.config import getenv_str
from users.user_repo import SurrealUserDatabase
from surrealdb import Surreal
from .config import settings
import logging




db: Surreal = None
# --- Lifecycle management ---
async def init_db() -> None:
    """Initialize SurrealDB connection on app startup."""
    
    global db
    db = AsyncSurreal(settings.SURREALDB_URL)
    try:
        await db.signin({"username": settings.SURREALDB_USER, "password": settings.SURREALDB_PASS})
    except Exception as e:
        raise Exception(f"Error initializing app database connection. Check your login credentials: {e}") from e

    try:
        await db.use(settings.SURREALDB_NS, settings.SURREALDB_DB)

        test_result = await db.query("INFO FOR DB")
    except Exception as e:
        raise Exception(f"Error initializing app database connection. Check your credentials: {e}") from e


async def close_db() -> None:
    """Close SurrealDB connection on app shutdown."""
    try:
        await db.close()
    except Exception as e:
        raise Exception("Error closing app database connection") from e


# --- FastAPI dependencies ---
async def get_db() -> AsyncSurreal:
    """Dependency to inject the Surreal client into routes."""
    if db is None:
        await init_db()
    yield db


async def get_user_db(db: AsyncSurreal = Depends(get_db)) -> SurrealUserDatabase:
    if db is None:
        await init_db()
    yield SurrealUserDatabase(db, "users")







