from __future__ import annotations

from typing import List, Optional, Dict
from pydantic import BaseModel, Field


class AccountHeader(BaseModel):
    account_name: str
    account_number: str
    opening_balance: float
    closing_balance: float
    metadata: Optional[Dict[str, str]] = None


class Transaction(BaseModel):
    trans_time: str
    value_date: Optional[str] = None
    description: str
    debit: float
    credit: float
    balance: float
    transaction_reference: Optional[str] = None
    counterparty: Optional[str] = None
    transaction_category: Optional[str] = None


class ExtractedStatement(BaseModel):
    header: AccountHeader
    transactions: List[Transaction] = Field(default_factory=list)


