from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import JSON, TIMESTAMP, Date, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TransactionRaw(Base):
    __tablename__ = "transactions_raw"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    blob: Mapped[dict] = mapped_column(JSON, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class TransactionCleaned(Base):
    __tablename__ = "transactions_cleaned"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    txn_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    merchant: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_desc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    merchant_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    quality_flags: Mapped[dict] = mapped_column(JSON, nullable=False, server_default=text("'{}'::jsonb"))


