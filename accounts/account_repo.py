from __future__ import annotations

from typing import Optional
from surrealdb import AsyncSurreal


class AccountRepo:
    def __init__(self, db: AsyncSurreal):
        self.db = db

    async def create_account(self, owner_id: str, header: dict, s3_raw_url: str) -> str:
        # Placeholder write; implement your SurrealDB schema as needed
        return "account_id"


