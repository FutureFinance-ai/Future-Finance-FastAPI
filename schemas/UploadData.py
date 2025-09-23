from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from datetime import date



class ExcelUploadResponse(BaseModel):
    filename: str
    content_type: str
    sheets: List[str]
    rows_per_sheet: Dict[str, int]


class CategorizedTransaction(BaseModel):
    date: date
    description: str
    amount: float
    type: str  # "credit" or "debit"
    category: str  # human readable category label
    subcategory: Optional[str] = None
    account: Optional[str] = None
    raw: Optional[Dict[str, str]] = None


class CleanedStatementDocument(BaseModel):
    account_id: Optional[str] = None
    statement_month: str = Field(..., description="YYYY-MM for the statement period")
    opening_balance: float
    closing_balance: float
    total_credits: float
    total_debits: float
    currency: Optional[str] = None
    transactions: List[CategorizedTransaction]
    metadata: Optional[Dict[str, str]] = None


class DocumentUploadResponse(BaseModel):
    account_name: str
    account_number: str
    total_credit: float
    total_debit: float
    opening_balance: float
    closing_balance: float
    number_of_transactions: int
    url: str
    filename: str



