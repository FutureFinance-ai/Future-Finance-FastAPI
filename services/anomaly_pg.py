from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest  # type: ignore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _feature(amount: float, txn_date: str) -> np.ndarray:
    # Simple features: amount, day_of_week (0-6)
    dow = datetime.fromisoformat(str(txn_date)).weekday()
    return np.array([amount, float(dow)], dtype=float)


async def score_anomaly_for_txn(session: AsyncSession, txn_id: str, history: int = 300) -> None:
    # Fetch context user and recent history
    tx = (
        await session.execute(
            text(
                """
                SELECT id, user_id, txn_date, amount
                FROM transactions_cleaned WHERE id = :id
                """
            ),
            {"id": txn_id},
        )
    ).mappings().first()
    if not tx:
        return
    rows = (
        await session.execute(
            text(
                """
                SELECT txn_date, amount
                FROM transactions_cleaned
                WHERE user_id = :uid
                ORDER BY txn_date DESC
                LIMIT :lim
                """
            ),
            {"uid": tx["user_id"], "lim": history},
        )
    ).mappings().all()
    if len(rows) < 20:
        return
    X = np.vstack([_feature(float(r["amount"]), str(r["txn_date"])) for r in rows])
    model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
    model.fit(X)
    score = float(model.decision_function([_feature(float(tx["amount"]), str(tx["txn_date"]))])[0])
    # Lower score = more anomalous; convert to positive anomaly_score
    anomaly_score = float(-score)
    reason = {
        "features": {"amount": float(tx["amount"]), "dow": datetime.fromisoformat(str(tx["txn_date"])).weekday()},
        "history_count": len(rows),
        "model": "isolation-forest-v1",
    }
    await session.execute(
        text(
            """
            INSERT INTO anomalies (txn_id, score, reason)
            VALUES (:txn_id, :score, :reason::jsonb)
            """
        ),
        {"txn_id": txn_id, "score": anomaly_score, "reason": json_dumps(reason)},
    )


