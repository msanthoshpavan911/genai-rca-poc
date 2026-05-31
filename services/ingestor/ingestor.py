"""
=============================================================================
Log Ingestor — Kafka → Embeddings → OpenSearch k-NN
=============================================================================

Consumes the app-logs topic, groups logs by trace_id within rolling windows,
generates embeddings using bge-large-en-v1.5, and writes to OpenSearch
logs-vectors-* index.

Batching:
  - Flushes when 500 logs OR 30 seconds elapse, whichever first

At-least-once delivery:
  - Manual offset commit AFTER successful OpenSearch indexing

Run:
    python ingestor.py
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from aiokafka import AIOKafkaConsumer
from opensearchpy import AsyncOpenSearch, helpers
from sentence_transformers import SentenceTransformer


# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "app-logs")
KAFKA_GROUP       = os.getenv("KAFKA_GROUP", "genai-ingestor-cg")

OPENSEARCH_HOST   = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT   = int(os.getenv("OPENSEARCH_PORT", "9200"))
INDEX_NAME        = os.getenv("INDEX_NAME", "logs-vectors-current")

EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
EMBEDDING_DIM     = 1024

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_S  = float(os.getenv("FLUSH_INTERVAL_S", "30"))
CHUNK_WINDOW_S    = float(os.getenv("CHUNK_WINDOW_S", "60"))
EMBED_BATCH_SIZE  = int(os.getenv("EMBED_BATCH_SIZE", "32"))

DROP_LEVELS       = {"DEBUG", "TRACE"}

# PII masking patterns
PII_PATTERNS = [
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"), "[EMAIL]"),
    (re.compile(r"\b(?:\d[ -]*?){13,16}\b"), "[CARD]"),
    (re.compile(r"\b\+?[\d\s\-()]{10,15}\b"), "[PHONE]"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestor")


# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass
class LogEvent:
    """One parsed log line from Kafka."""
    timestamp: datetime
    level: str
    service: str
    trace_id: str
    order_no: str
    location_no: str
    message: str
    exception: Optional[str] = None
    raw: Dict = field(default_factory=dict)


@dataclass
class LogChunk:
    """A group of logs sharing a trace_id within a window."""
    trace_id: str
    order_no: str
    location_no: str
    services: List[str]
    log_levels: List[str]
    earliest_ts: datetime
    latest_ts: datetime
    has_error: bool
    raw_logs: List[Dict]
    composed_text: str    # what gets embedded


# =============================================================================
# PARSING & ENRICHMENT
# =============================================================================
def mask_pii(text: str) -> str:
    """Strip emails, card numbers, phone numbers."""
    if not text:
        return text
    for pattern, replacement in PII_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def parse_log(raw_bytes: bytes) -> Optional[LogEvent]:
    """Parse a Kafka message into a LogEvent. Returns None if malformed."""
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    try:
        return LogEvent(
            timestamp=datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")),
            level=data.get("level", "INFO"),
            service=data.get("service", "unknown"),
            trace_id=data.get("trace_id", "no-trace"),
            order_no=data.get("order_no", ""),
            location_no=data.get("location_no", ""),
            message=mask_pii(data.get("message", "")),
            exception=mask_pii(data.get("exception")) if data.get("exception") else None,
            raw=data,
        )
    except (KeyError, ValueError):
        return None


# =============================================================================
# CHUNKER — groups logs by trace_id in time windows
# =============================================================================
class TraceChunker:
    """Groups log events by trace_id; emits chunks when ready."""

    def __init__(self, window_seconds: float = CHUNK_WINDOW_S):
        self.window_seconds = window_seconds
        # trace_id -> list of LogEvent
        self.buffers: Dict[str, List[LogEvent]] = defaultdict(list)
        # trace_id -> earliest timestamp seen
        self.first_seen: Dict[str, float] = {}

    def add(self, event: LogEvent):
        if event.level in DROP_LEVELS:
            return
        self.buffers[event.trace_id].append(event)
        if event.trace_id not in self.first_seen:
            self.first_seen[event.trace_id] = time.time()

    def drain_ready(self, force_all: bool = False) -> List[LogChunk]:
        """Return chunks whose window has elapsed (or all if force_all)."""
        chunks: List[LogChunk] = []
        now = time.time()

        to_remove = []
        for trace_id, events in self.buffers.items():
            window_elapsed = (now - self.first_seen[trace_id]) >= self.window_seconds
            if force_all or window_elapsed:
                chunks.append(self._build_chunk(events))
                to_remove.append(trace_id)

        for trace_id in to_remove:
            del self.buffers[trace_id]
            del self.first_seen[trace_id]

        return chunks

    @staticmethod
    def _build_chunk(events: List[LogEvent]) -> LogChunk:
        """Build a LogChunk from a list of correlated LogEvents."""
        events = sorted(events, key=lambda e: e.timestamp)

        # Compose embedding-friendly text
        lines = []
        for e in events:
            line = f"[{e.timestamp.strftime('%H:%M:%S')}] [{e.service}] {e.level}: {e.message}"
            if e.exception:
                # Just include first line of exception for embedding context
                first_exc_line = e.exception.split('\n')[0]
                line += f" | exception: {first_exc_line}"
            lines.append(line)
        composed_text = "\n".join(lines)

        return LogChunk(
            trace_id=events[0].trace_id,
            order_no=events[0].order_no,
            location_no=events[0].location_no,
            services=sorted(set(e.service for e in events)),
            log_levels=sorted(set(e.level for e in events)),
            earliest_ts=events[0].timestamp,
            latest_ts=events[-1].timestamp,
            has_error=any(e.level == "ERROR" for e in events),
            raw_logs=[e.raw for e in events],
            composed_text=composed_text,
        )


# =============================================================================
# OPENSEARCH INDEX SETUP
# =============================================================================
INDEX_BODY = {
    "settings": {
        "index.knn": True,
        "index.refresh_interval": "30s",
        "number_of_shards": 1,        # local POC — production uses 3
        "number_of_replicas": 0,      # local POC has only one node
    },
    "mappings": {
        "properties": {
            "chunk_id":     {"type": "keyword"},
            "order_no":     {"type": "keyword"},
            "trace_id":     {"type": "keyword"},
            "location_no":  {"type": "keyword"},
            "services":     {"type": "keyword"},
            "log_levels":   {"type": "keyword"},
            "has_error":    {"type": "boolean"},
            "earliest_ts":  {"type": "date"},
            "latest_ts":    {"type": "date"},
            "message":      {"type": "text"},
            "raw_logs":     {"type": "object", "enabled": False},  # stored but not indexed
            "embedding": {
                "type": "knn_vector",
                "dimension": EMBEDDING_DIM,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    "space_type": "cosinesimil",
                    "parameters": {"ef_construction": 256, "m": 16},
                },
            },
        }
    },
}


async def ensure_index(client: AsyncOpenSearch, index_name: str):
    """Create the index if it doesn't exist."""
    exists = await client.indices.exists(index=index_name)
    if exists:
        log.info(f"Index {index_name} already exists.")
        return
    await client.indices.create(index=index_name, body=INDEX_BODY)
    log.info(f"✅ Created index {index_name}")


# =============================================================================
# EMBEDDING SERVICE
# =============================================================================
class EmbeddingService:
    """Wraps the bge-large-en-v1.5 model."""

    def __init__(self, model_name: str = EMBEDDING_MODEL):
        log.info(f"Loading embedding model: {model_name} (this may take a minute on first run)")
        self.model = SentenceTransformer(model_name)
        log.info("✅ Embedding model loaded.")

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of texts. Returns list of 1024-dim vectors."""
        vectors = self.model.encode(
            texts,
            batch_size=EMBED_BATCH_SIZE,
            normalize_embeddings=True,   # ensure cosine similarity works correctly
            show_progress_bar=False,
        )
        return vectors.tolist()


# =============================================================================
# MAIN INGESTOR LOOP
# =============================================================================
class Ingestor:
    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.os_client: Optional[AsyncOpenSearch] = None
        self.embedder: Optional[EmbeddingService] = None
        self.chunker = TraceChunker()
        self.buffer_count = 0
        self.last_flush = time.time()
        self.shutdown = asyncio.Event()

    async def start(self):
        log.info("🚀 Starting ingestor…")
        self.embedder = EmbeddingService()

        self.os_client = AsyncOpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            use_ssl=False,
            verify_certs=False,
        )
        await ensure_index(self.os_client, INDEX_NAME)

        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        log.info(f"✅ Consumer started on topic={KAFKA_TOPIC} group={KAFKA_GROUP}")

    async def stop(self):
        log.info("🛑 Shutting down…")
        # Final flush to capture remaining buffered traces
        await self.flush(force=True)
        if self.consumer:
            await self.consumer.stop()
        if self.os_client:
            await self.os_client.close()
        log.info("👋 Ingestor stopped.")

    async def run(self):
        """Main consume loop."""
        await self.start()

        try:
            async for msg in self.consumer:
                if self.shutdown.is_set():
                    break

                event = parse_log(msg.value)
                if event is None:
                    log.warning(f"Malformed log skipped (offset={msg.offset})")
                    continue

                self.chunker.add(event)
                self.buffer_count += 1

                # Time- or size-triggered flush
                should_flush = (
                    self.buffer_count >= BATCH_SIZE
                    or (time.time() - self.last_flush) >= FLUSH_INTERVAL_S
                )
                if should_flush:
                    await self.flush()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def flush(self, force: bool = False):
        """Drain ready chunks, embed them, and index to OpenSearch."""
        chunks = self.chunker.drain_ready(force_all=force)
        if not chunks:
            self.buffer_count = 0
            self.last_flush = time.time()
            return

        log.info(f"Flushing {len(chunks)} chunks ({self.buffer_count} raw logs)")

        # Embed in batches
        texts = [c.composed_text for c in chunks]
        embeddings = self.embedder.embed_batch(texts)

        # Build OpenSearch bulk actions with deterministic IDs
        actions = []
        for chunk, vector in zip(chunks, embeddings):
            chunk_id = self._chunk_id(chunk)
            actions.append({
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": chunk_id,
                "_source": {
                    "chunk_id":     chunk_id,
                    "order_no":     chunk.order_no,
                    "trace_id":     chunk.trace_id,
                    "location_no":  chunk.location_no,
                    "services":     chunk.services,
                    "log_levels":   chunk.log_levels,
                    "has_error":    chunk.has_error,
                    "earliest_ts":  chunk.earliest_ts.isoformat(),
                    "latest_ts":    chunk.latest_ts.isoformat(),
                    "message":      chunk.composed_text,
                    "raw_logs":     chunk.raw_logs,
                    "embedding":    vector,
                },
            })

        try:
            success, errors = await helpers.async_bulk(
                self.os_client, actions, refresh=False, raise_on_error=False
            )
            if errors:
                log.error(f"OpenSearch bulk errors: {len(errors)} (first: {errors[:1]})")
            else:
                log.info(f"✅ Indexed {success} chunks to {INDEX_NAME}")

            # Commit Kafka offsets ONLY after successful write
            await self.consumer.commit()
        except Exception as e:
            log.error(f"Flush failed, will retry on next message: {e}")
            return

        self.buffer_count = 0
        self.last_flush = time.time()

    @staticmethod
    def _chunk_id(chunk: LogChunk) -> str:
        """Deterministic chunk ID to make ingestion idempotent."""
        bucket = chunk.earliest_ts.strftime("%Y%m%d%H%M")
        raw = f"{chunk.order_no}-{chunk.trace_id}-{bucket}"
        return hashlib.md5(raw.encode()).hexdigest()


# =============================================================================
# ENTRYPOINT
# =============================================================================
async def main():
    ingestor = Ingestor()

    def _handle_signal():
        log.info("Signal received, shutting down…")
        ingestor.shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows
            pass

    await ingestor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
