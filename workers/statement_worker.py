from __future__ import annotations

import json
from arq import create_pool
from arq.connections import RedisSettings
from typing import Any

from settings.config import settings
from storage.s3_client import S3Client
from analysis_service.analysis_service import get_analysis_service
from schemas.extraction import ExtractedStatement, AccountHeader
from surrealdb import AsyncSurreal
from upload_service.upload_repo import UploadRepo
from upload_service.models import TransactionIn


async def get_redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")


class WorkerSettings:
    functions = ["process_statement"]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")


async def process_statement(ctx: dict[str, Any], s3_key: str, user_id: str, account_hint: str | None = None) -> str:
    # Download raw JSON from S3
    s3 = S3Client()
    bucket, key = s3_key.replace("s3://", "").split("/", 1)
    # simple GET via presigned URL not shown; assume you have permissions for direct get_object in your environment
    # In a real setup, we'd use s3 client get_object and read body
    # For scaffolding, we assume the worker has the raw json string (skipped)
    raw_json = "{}"

    data = json.loads(raw_json)
    header = AccountHeader(
        account_name=data.get("account_name", ""),
        account_number=data.get("account_number", ""),
        opening_balance=float(data.get("opening_balance", 0.0) or 0.0),
        closing_balance=float(data.get("closing_balance", 0.0) or 0.0),
    )
    statement = ExtractedStatement(header=header, transactions=data.get("transactions", []))

    # Optional: normalize via analysis service (kept as-is)
    analysis = get_analysis_service()
    _ = await analysis.transactions_to_dataframe({"transactions": [t.model_dump() for t in statement.transactions]})

    # Persist to SurrealDB using bulk insert
    db = AsyncSurreal(settings.SURREALDB_URL)
    await db.signin({"username": settings.SURREALDB_USER, "password": settings.SURREALDB_PASS})
    await db.use(settings.SURREALDB_NS, settings.SURREALDB_DB)

    repo = UploadRepo(db)
    txns = [
        TransactionIn(
            trans_time=tx.get("trans_time") or tx.get("transaction_date"),
            value_date=tx.get("value_date") or tx.get("transaction_date"),
            description=tx.get("description") or tx.get("transaction_description", ""),
            debit=tx.get("debit", 0.0),
            credit=tx.get("credit", 0.0),
            balance=tx.get("balance"),
        )
        for tx in data.get("transactions", [])
    ]

    await repo.save_user_upload(user_id=user_id, account_header=header, transactions=txns, s3_url=s3_key)
    await db.close()
    return "ok"


