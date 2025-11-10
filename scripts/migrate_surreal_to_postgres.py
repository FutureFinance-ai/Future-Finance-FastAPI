from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, List

from surrealdb import AsyncSurreal
from settings.config import settings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text


async def export_users(surreal: AsyncSurreal) -> List[dict[str, Any]]:
    try:
        res = await surreal.query("SELECT * FROM users;")
        return res[0] if res and isinstance(res[0], list) else []
    except Exception:
        return []


async def export_transactions(surreal: AsyncSurreal) -> List[dict[str, Any]]:
    # Surreal schema may differ; attempt common table name
    queries = [
        "SELECT * FROM transaction;",  # common in this codebase
        "SELECT * FROM transactions;",
    ]
    for q in queries:
        try:
            res = await surreal.query(q)
            if res and isinstance(res[0], list):
                return res[0]
        except Exception:
            continue
    return []


async def main() -> None:
    surreal = AsyncSurreal(settings.SURREALDB_URL)
    await surreal.signin({"username": settings.SURREALDB_USER, "password": settings.SURREALDB_PASS})
    await surreal.use(settings.SURREALDB_NS, settings.SURREALDB_DB)

    engine = create_async_engine(settings.PGBOUNCER_DSN or settings.POSTGRES_DSN)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    users = await export_users(surreal)
    txs = await export_transactions(surreal)
    print(f"Exported users={len(users)} txs={len(txs)}")

    async with session_factory() as session:  # type: AsyncSession
        # Load users
        for u in users:
            email = u.get("email") or u.get("id") or f"user_{uuid.uuid4()}@example.com"
            hashed_password = u.get("hashed_password") or u.get("password") or "!"
            await session.execute(
                text(
                    """
                    INSERT INTO users (email, hashed_password)
                    VALUES (:email, :hp)
                    ON CONFLICT (email) DO NOTHING
                    """
                ),
                {"email": email, "hp": hashed_password},
            )
        # Load raw transactions
        for t in txs:
            user_id = t.get("account", {}).get("owner") or t.get("user_id")
            # We cannot map Surreal record links safely; leave user_id NULL if not parseable
            if not user_id:
                continue
            # Best-effort blob
            await session.execute(
                text(
                    """
                    INSERT INTO transactions_raw (user_id, source, blob)
                    VALUES (:user_id::uuid, :source, :blob::jsonb)
                    """
                ),
                {
                    "user_id": str(user_id).split(":")[-1],  # handle type::thing() id format
                    "source": "surreal_migration",
                    "blob": json.dumps(t),
                },
            )
        await session.commit()

    await surreal.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())


