from __future__ import annotations

from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from settings.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _dsn() -> str:
    # Prefer pgbouncer when configured
    return settings.PGBOUNCER_DSN or settings.POSTGRES_DSN


def get_engine() -> AsyncEngine:
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            _dsn(),
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        get_engine()
    assert _session_factory is not None
    return _session_factory


async def get_async_session() -> AsyncIterator[AsyncSession]:
    """
    FastAPI dependency for request-scoped AsyncSession.
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


async def init_postgres() -> None:
    """
    Initialize the engine and verify connectivity.
    """
    engine = get_engine()
    # Optionally test a simple connection
    async with engine.begin() as conn:  # no-op transaction for connectivity test
        await conn.run_sync(lambda _: None)


async def close_postgres() -> None:
    """
    Dispose the engine on application shutdown.
    """
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None


