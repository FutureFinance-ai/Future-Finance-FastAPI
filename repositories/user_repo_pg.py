from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth.tables import UserTable as User


class UserRepositoryPg:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        stmt = select(User).where(User.id == user_id)
        res = await self._session.execute(stmt)
        return res.scalars().first()

    async def get_by_email(self, email: str) -> Optional[User]:
        stmt = select(User).where(User.email == email)
        res = await self._session.execute(stmt)
        return res.scalars().first()

    async def create(self, email: str, hashed_password: str) -> User:
        user = User(email=email, hashed_password=hashed_password)
        self._session.add(user)
        await self._session.flush()
        return user


