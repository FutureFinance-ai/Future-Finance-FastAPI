from __future__ import annotations

from fastapi import Depends
from surrealdb import AsyncSurreal
from users.user_repo import SurrealUserDatabase
from .config import settings
import pathlib




db = None
# --- Lifecycle management ---
async def init_db():
    """Initialize SurrealDB connection on app startup."""
    
    global db
    db = AsyncSurreal(settings.SURREALDB_URL)
    try:
        await db.signin({
            "username": settings.SURREALDB_USER, 
            "password": settings.SURREALDB_PASS
            })
    except Exception as e:
        raise Exception(f"Error initializing app database connection. Check your login credentials: {e}") from e

    try:
        await db.use(settings.SURREALDB_NS, settings.SURREALDB_DB)

        # test_result = await db.query("INFO FOR DB")
    except Exception as e:
        raise Exception(f"Error initializing app database connection. Check your credentials: {e}") from e

    # Load schema once on startup (idempotent DEFINE statements)
    try:
        schema_path = pathlib.Path(__file__).resolve().parents[1] / "settings" / "surreal" / "schema.surql"
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        await db.query(schema_sql)
    except Exception as e:
        # Do not crash the app if schema already exists; log in real setup
        pass


async def close_db():
    """Close SurrealDB connection on app shutdown."""
    try:
        await db.close()  # type: ignore
    except Exception as e:
        raise Exception("Error closing app database connection") from e


# --- FastAPI dependencies ---
async def get_db():
    """Return the Surreal client for DI and direct usage in tests."""
    if db is None:
        await init_db()
    return db  # type: ignore


async def get_user_db(db: AsyncSurreal = Depends(get_db)):
    if db is None:
        await init_db()
    yield SurrealUserDatabase(db, "users")  # type: ignore







