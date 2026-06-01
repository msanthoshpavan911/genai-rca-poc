"""Shared embedding utility — Google text-embedding-004 (768-dim) via REST."""
import os
from typing import List

import requests

GOOGLE_API_KEY  = os.getenv("GOOGLE_API_KEY")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")

_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"


def _embed_one(text: str) -> List[float]:
    if not GOOGLE_API_KEY:
        raise RuntimeError(
            "GOOGLE_API_KEY environment variable is not set. "
            "Run: $env:GOOGLE_API_KEY='your_key' before starting the server."
        )
    url = f"{_BASE_URL}/{EMBEDDING_MODEL}:embedContent"
    resp = requests.post(
        url,
        headers={"x-goog-api-key": GOOGLE_API_KEY},
        json={
            "model": f"models/{EMBEDDING_MODEL}",
            "content": {"parts": [{"text": text}]},
            "taskType": "RETRIEVAL_DOCUMENT",
        },
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Embedding API error {resp.status_code}: {resp.text}")
    return resp.json()["embedding"]["values"]


def embed(texts: List[str]) -> List[List[float]]:
    """Embed a list of texts, return list of 768-dim vectors."""
    return [_embed_one(t) for t in texts]


def embed_one(text: str) -> List[float]:
    """Embed a single text, return one 768-dim vector."""
    return _embed_one(text)
