from __future__ import annotations

from typing import List, Tuple
from settings.db import get_service_db
from ai_services.categorization import MLCategorizer


async def load_corrections(limit: int = 10000) -> Tuple[List[str], List[str]]:
    """
    Load user-corrected training data from SurrealDB.
    Returns descriptions and labels.
    """
    db = await get_service_db()
    query = """
    SELECT description, user_corrected_category
    FROM transaction
    WHERE description != NONE
      AND user_corrected_category != NONE
    LIMIT $limit;
    """
    res = await db.query(query, {"limit": limit})
    rows = res[0]
    await db.close()
    descriptions = [r["description"] for r in rows if r.get("description")]
    labels = [r["user_corrected_category"] for r in rows if r.get("user_corrected_category")]
    # align lengths
    n = min(len(descriptions), len(labels))
    return descriptions[:n], labels[:n]


async def train_model_from_corrections() -> dict:
    descriptions, labels = await load_corrections()
    if not descriptions or not labels:
        return {"trained": False, "reason": "no_data"}
    ml = MLCategorizer()
    ml.train(descriptions, labels)
    ml.save_model()
    return {"trained": True, "samples": len(descriptions)}


