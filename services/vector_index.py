from __future__ import annotations

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, HnswConfigDiff, OptimizersConfigDiff, VectorParams
from settings.config import settings

_client: QdrantClient | None = None


def get_qdrant() -> QdrantClient:
    global _client
    if _client is None:
        _client = QdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    return _client


def ensure_transactions_collection(vector_size: int = 1536) -> None:
    """
    Idempotently create the transactions embeddings collection.
    """
    client = get_qdrant()
    collection = "transactions_embeddings"
    try:
        exists = client.get_collection(collection_name=collection)
        # If exists, assume OK; advanced diffs could be added later
        return
    except Exception:
        pass
    client.recreate_collection(
        collection_name=collection,
        vectors=VectorParams(size=vector_size, distance=Distance.COSINE),
        optimizers_config=OptimizersConfigDiff(memmap_threshold=20000),
        hnsw_config=HnswConfigDiff(ef_construct=128, m=16),
    )


