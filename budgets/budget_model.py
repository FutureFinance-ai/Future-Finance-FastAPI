from __future__ import annotations

from pydantic import BaseModel
from datetime import datetime


class Budget(BaseModel):
    user_id: str
    transaction_category: str
    budget: float
    month: datetime  # first day of month
    currency: str = "NGN"


