from __future__ import annotations

from typing import List
from surrealdb import AsyncSurreal
from budgets.budget_model import Budget


class BudgetRepo:
    def __init__(self, db: AsyncSurreal):
        self.db = db

    async def upsert(self, budget: Budget) -> Budget:
        # Placeholder upsert; implement SurrealDB schema as needed
        return budget

    async def list_for_user(self, user_id: str) -> List[Budget]:
        return []


