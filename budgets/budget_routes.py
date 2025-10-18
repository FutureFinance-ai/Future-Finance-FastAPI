from __future__ import annotations

from fastapi import APIRouter, Depends
from typing import List

from budgets.budget_model import Budget
from budgets.budget_repo import BudgetRepo
from config.db import get_db
from surrealdb import AsyncSurreal


router = APIRouter(prefix="/budgets", tags=["budgets"])


def get_budget_repo(db: AsyncSurreal = Depends(get_db)) -> BudgetRepo:
    return BudgetRepo(db)


@router.get("/", response_model=List[Budget])
async def list_budgets(repo: BudgetRepo = Depends(get_budget_repo)) -> List[Budget]:
    # TODO: derive user_id from auth
    return await repo.list_for_user(user_id="me")


@router.post("/", response_model=Budget)
async def upsert_budget(budget: Budget, repo: BudgetRepo = Depends(get_budget_repo)) -> Budget:
    return await repo.upsert(budget)


