from __future__ import annotations

from typing import Any

from arq.connections import RedisSettings

from settings.config import settings
from db.postgres import get_session_factory
from sqlalchemy import text
from services.vector_index import get_qdrant
from services.llm_cleaning import clean_with_llm
from services.embeddings import embed_texts, upsert_transaction_embedding
from db.models import TransactionCleaned
from services.categorize_pg import categorize_txn_and_write
from services.anomaly_pg import score_anomaly_for_txn
import json as _json


def json_dumps(obj: dict) -> str:
    return _json.dumps(obj, ensure_ascii=False)


async def process_outbox_events(ctx: dict[str, Any], batch_size: int = 100) -> dict:
    """
    Pull unprocessed outbox events and dispatch to specific handlers.
    Processing:
      - TRANSACTION_INGESTED -> run cleaning (implemented next task)
      - TRANSACTION_CLEANED  -> compute embeddings and upsert to Qdrant
    """
    session_factory = get_session_factory()
    processed = 0
    async with session_factory() as session:
        # Lock a batch of rows for processing
        rows = (
            await session.execute(
                text(
                    """
                    SELECT id, kind, aggregate_id, payload
                    FROM outbox_events
                    WHERE processed_at IS NULL
                    ORDER BY id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT :lim
                    """
                ),
                {"lim": batch_size},
            )
        ).mappings().all()

        for row in rows:
            kind: str = row["kind"]
            if kind == "TRANSACTION_INGESTED":
                payload = row["payload"]
                try:
                    cleaned = clean_with_llm(payload.get("blob", {}))
                    # Insert cleaned row
                    result = await session.execute(
                        text(
                            """
                            INSERT INTO transactions_cleaned
                                (raw_id, user_id, txn_date, amount, currency, merchant, raw_description, normalized_desc, merchant_id, quality_flags)
                            VALUES
                                (:raw_id, :user_id, :txn_date, :amount, :currency, :merchant, :raw_description, :normalized_desc, :merchant_id, :quality_flags)
                            RETURNING id
                            """
                        ),
                        {
                            "raw_id": payload["raw_id"],
                            "user_id": payload["user_id"],
                            "txn_date": cleaned.txn_date.isoformat(),
                            "amount": cleaned.amount,
                            "currency": cleaned.currency,
                            "merchant": cleaned.merchant,
                            "raw_description": cleaned.raw_description,
                            "normalized_desc": cleaned.normalized_desc,
                            "merchant_id": None,
                            "quality_flags": {},
                        },
                    )
                    new_id = result.scalar_one()
                    # Emit next-stage event
                    await session.execute(
                        text(
                            """
                            INSERT INTO outbox_events (kind, aggregate_id, payload)
                            VALUES (:kind, :agg, :payload::jsonb)
                            """
                        ),
                        {
                            "kind": "TRANSACTION_CLEANED",
                            "agg": new_id,
                            "payload": json_dumps(
                                {
                                    "txn_id": str(new_id),
                                    "user_id": payload["user_id"],
                                    "workspace_id": payload.get("workspace_id"),
                                }
                            ),
                        },
                    )
                except Exception:
                    # In production, add DLQ / retries
                    pass
                await session.execute(text("UPDATE outbox_events SET processed_at = now() WHERE id = :id"), {"id": row["id"]})
                processed += 1
            elif kind == "TRANSACTION_CLEANED":
                payload = row["payload"]
                # Fetch text fields for embedding
                tx = (
                    await session.execute(
                        text(
                            """
                            SELECT id, user_id, txn_date, amount, currency, merchant, raw_description, normalized_desc
                            FROM transactions_cleaned WHERE id = :id
                            """
                        ),
                        {"id": payload["txn_id"]},
                    )
                ).mappings().first()
                if tx:
                    text_blob = " ".join(
                        [
                            str(tx.get("normalized_desc") or "")[:500],
                            str(tx.get("merchant") or "")[:120],
                            str(tx.get("raw_description") or "")[:500],
                        ]
                    ).strip()
                    vectors = embed_texts([text_blob])
                    vector = vectors[0]
                    upsert_transaction_embedding(
                        txn_id=str(tx["id"]),
                        user_id=payload["user_id"],
                        workspace_id=payload.get("workspace_id"),
                        vector=vector,
                        payload={
                            "txn_date": str(tx["txn_date"]),
                            "amount": float(tx["amount"]),
                            "currency": tx["currency"],
                            "merchant": tx.get("merchant"),
                        },
                    )
                    # Categorization prediction
                    await categorize_txn_and_write(session, str(tx["id"]))
                    # Anomaly scoring
                    await score_anomaly_for_txn(session, str(tx["id"]))
                await session.execute(text("UPDATE outbox_events SET processed_at = now() WHERE id = :id"), {"id": row["id"]})
                processed += 1
            else:
                # Unknown kinds: mark processed to avoid blocking
                await session.execute(text("UPDATE outbox_events SET processed_at = now() WHERE id = :id"), {"id": row["id"]})
                processed += 1
        await session.commit()
    return {"processed": processed}


class WorkerSettings:
    functions = [process_outbox_events]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")


