from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from ai_services.categorization import CategorizationService


async def categorize_txn_and_write(session: AsyncSession, txn_id: str) -> None:
    row = (
        await session.execute(
            text(
                """
                SELECT id, normalized_desc, raw_description
                FROM transactions_cleaned
                WHERE id = :id
                """
            ),
            {"id": txn_id},
        )
    ).mappings().first()
    if not row:
        return
    desc = (row.get("normalized_desc") or "") + " " + (row.get("raw_description") or "")
    svc = CategorizationService()
    label, _, source = svc.categorize_description(desc)
    # Write prediction
    await session.execute(
        text(
            """
            INSERT INTO model_predictions (txn_id, kind, version, label, score)
            VALUES (:txn_id, 'category', :version, :label, :score)
            """
        ),
        {"txn_id": txn_id, "version": f"baseline-{source}-v1", "label": label, "score": 1.0 if label != "Uncategorized" else 0.0},
    )

