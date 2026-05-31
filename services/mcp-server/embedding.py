"""Shared embedding utility — bge-large-en-v1.5."""
import os
from typing import List
from sentence_transformers import SentenceTransformer

_model = None

def get_model():
    global _model
    if _model is None:
        model_name = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
        _model = SentenceTransformer(model_name)
    return _model


def embed(texts: List[str]) -> List[List[float]]:
    """Embed a list of texts, return list of 1024-dim vectors."""
    model = get_model()
    vectors = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    return vectors.tolist()


def embed_one(text: str) -> List[float]:
    """Embed a single text, return one 1024-dim vector."""
    return embed([text])[0]
