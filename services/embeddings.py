from __future__ import annotations

from typing import Iterable, List

from openai import OpenAI
from settings.config import settings
from services.vector_index import get_qdrant
from qdrant_client.models import PointStruct


def _client() -> OpenAI:
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not configured")
    return OpenAI(api_key=settings.OPENAI_API_KEY)


def embed_texts(texts: Iterable[str]) -> List[List[float]]:
    client = _client()
    # Using text-embedding-3-small (1536 dims)
    resp = client.embeddings.create(model="text-embedding-3-small", input=list(texts))
    return [d.embedding for d in resp.data]


def upsert_transaction_embedding(
    txn_id: str,
    user_id: str,
    workspace_id: str | None,
    vector: list[float],
    payload: dict,
) -> None:
    client = get_qdrant()
    client.upsert(
        collection_name="transactions_embeddings",
        points=[
            PointStruct(
                id=txn_id,
                vector=vector,
                payload={
                    "txn_id": txn_id,
                    "user_id": user_id,
                    "workspace_id": workspace_id,
                    **payload,
                },
            )
        ],
    )


