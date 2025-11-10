from __future__ import annotations

import uuid
from datetime import date
from typing import Iterable, Optional

from sqlalchemy import Select, and_, desc, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import TransactionCleaned, TransactionRaw


class TransactionRepositoryPg:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # Raw ingestion
    async def create_raw(self, user_id: uuid.UUID, source: str, blob: dict) -> TransactionRaw:
        raw = TransactionRaw(user_id=user_id, source=source, blob=blob)
        self._session.add(raw)
        await self._session.flush()
        return raw

    # Cleaned transactions
    async def create_cleaned(
        self,
        raw_id: uuid.UUID,
        user_id: uuid.UUID,
        txn_date: date,
        amount: float,
        currency: str,
        merchant: Optional[str],
        raw_description: Optional[str],
        normalized_desc: Optional[str],
        merchant_id: Optional[uuid.UUID],
        quality_flags: dict | None = None,
    ) -> TransactionCleaned:
        cleaned = TransactionCleaned(
            raw_id=raw_id,
            user_id=user_id,
            txn_date=txn_date,
            amount=amount,
            currency=currency,
            merchant=merchant,
            raw_description=raw_description,
            normalized_desc=normalized_desc,
            merchant_id=merchant_id,
            quality_flags=quality_flags or {},
        )
        self._session.add(cleaned)
        await self._session.flush()
        return cleaned

    async def list_cleaned_for_user(
        self,
        user_id: uuid.UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
    ) -> list[TransactionCleaned]:
        stmt: Select[tuple[TransactionCleaned]] = select(TransactionCleaned).where(TransactionCleaned.user_id == user_id)
        if start_date:
            stmt = stmt.where(TransactionCleaned.txn_date >= start_date)
        if end_date:
            stmt = stmt.where(TransactionCleaned.txn_date <= end_date)
        stmt = stmt.order_by(desc(TransactionCleaned.txn_date)).limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())


