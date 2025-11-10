from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class CleanedTransaction(BaseModel):
    txn_date: date
    amount: float
    currency: str = Field(min_length=3, max_length=8)
    merchant: Optional[str] = None
    raw_description: Optional[str] = None
    normalized_desc: Optional[str] = None

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v: str) -> str:
        return v.upper()


