from __future__ import annotations

from pydantic import BaseModel, field_validator
from typing import Optional, List
from datetime import datetime


class AccountHeader(BaseModel):
    account_name: str
    account_number: str
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    currency: str = "NGN"

    def to_minor_units(self) -> tuple[int, int]:
        opening = int(round((self.opening_balance or 0.0) * 100))
        closing = int(round((self.closing_balance or 0.0) * 100))
        return opening, closing


class TransactionIn(BaseModel):
    trans_time: Optional[str] = None
    value_date: str
    description: str
    debit: Optional[float] = 0.0
    credit: Optional[float] = 0.0
    balance: Optional[float] = None

    @field_validator("trans_time")
    @classmethod
    def _strip_empty_trans_time(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v2 = v.strip()
        return v2 if v2 else None

    def to_db_row(self) -> dict:
        # parse datetimes to UTC isoformat
        def parse_dt(s: Optional[str]) -> Optional[str]:
            if not s:
                return None
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                try:
                    dt = datetime.strptime(s, "%Y %b %d")
                except Exception:
                    return None
            return dt.astimezone().astimezone(tz=None).isoformat()

        trans_time_iso = parse_dt(self.trans_time)
        value_date_iso = parse_dt(self.value_date)

        debit = float(self.debit or 0.0)
        credit = float(self.credit or 0.0)
        amount = credit - abs(debit)
        balance_minor = int(round(float(self.balance or 0.0) * 100)) if self.balance is not None else None

        row = {
            "trans_time": trans_time_iso,
            "value_date": value_date_iso,
            "description": self.description.strip(),
            "amount_minor": int(round(amount * 100)),
            "balance_minor": balance_minor,
        }
        return row


class UploadBatch(BaseModel):
    user_id: str
    header: AccountHeader
    transactions: List[TransactionIn]
    s3_url: str
    upload_id: Optional[str] = None


