from __future__ import annotations

from typing import AsyncIterator

from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase
from sqlalchemy.ext.asyncio import AsyncSession

from db.postgres import get_session_factory
from .tables import UserTable


async def get_user_db() -> AsyncIterator[SQLAlchemyUserDatabase]:
    session_factory = get_session_factory()
    async with session_factory() as session:  # type: AsyncSession
        yield SQLAlchemyUserDatabase(session, UserTable)


