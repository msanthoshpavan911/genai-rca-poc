"""
Seed the incidents-historical index with mock past RCAs.

Run AFTER infra is up and BEFORE testing find_similar_incidents.

Usage:
    python seed_incidents.py
"""

import asyncio
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "mcp-server"))

from opensearchpy import AsyncOpenSearch, helpers
from embedding import embed


OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
INDEX_NAME = "incidents-historical"

INCIDENTS = [
    {
        "incident_id": "INC-2024-0418",
        "summary": "Payment gateway timeouts spike during PSP outage",
        "root_cause": "Stripe PSP regional outage caused timeout cascade on payment-service. Circuit breaker tripped after 50% error rate threshold breached, blocking new payment attempts.",
        "resolution": "Failed over to secondary PSP provider. Increased connection pool from 50 to 100. Tuned circuit breaker to open at 30% error rate instead of 50% for faster failover.",
        "keywords": ["payment timeout", "PSP gateway", "circuit breaker", "stripe outage"],
    },
    {
        "incident_id": "INC-2024-0512",
        "summary": "Database connection pool exhausted under load",
        "root_cause": "HikariCP pool maxed out at 50 connections during peak traffic. Long-running queries from a misbehaving cron job held connections open beyond their typical lifespan.",
        "resolution": "Identified and killed runaway batch job. Increased HikariCP max size to 100. Added query timeout of 30 seconds. Added alerting on pool utilization > 80%.",
        "keywords": ["HikariCP", "connection pool", "JDBC", "database unavailable"],
    },
    {
        "incident_id": "INC-2024-0623",
        "summary": "NullPointerException in PaymentValidator after schema migration",
        "root_cause": "A recent schema migration nullified the payment_method column for legacy records. PaymentValidator did not null-check before calling getCardNumber().",
        "resolution": "Added null guards in PaymentValidator. Backfilled null payment_method values with 'LEGACY'. Added unit tests for null inputs.",
        "keywords": ["NullPointerException", "PaymentValidator", "schema migration", "null check"],
    },
    {
        "incident_id": "INC-2024-0701",
        "summary": "Inventory cache stale leading to oversells",
        "root_cause": "Redis cache TTL of 5 minutes was too long during flash sale; multiple orders saw same 'available' count and over-committed inventory.",
        "resolution": "Reduced cache TTL to 30 seconds for sale events. Added optimistic locking on inventory reservation. Implemented async cache invalidation on every inventory mutation.",
        "keywords": ["inventory mismatch", "stale cache", "oversell", "Redis TTL"],
    },
    {
        "incident_id": "INC-2024-0815",
        "summary": "Fraud detection false positives blocking legitimate orders",
        "root_cause": "New velocity rule blocked customers with > 3 orders in 10 min. Did not account for repeat enterprise customers with bulk orders.",
        "resolution": "Added customer-tier exemption list. Tuned velocity threshold per customer segment. Added manual review queue for blocked orders.",
        "keywords": ["fraud detection", "false positive", "velocity rule", "blocked order"],
    },
    {
        "incident_id": "INC-2024-0902",
        "summary": "Carrier API cascading 503 failures",
        "root_cause": "Primary carrier FedEx had datacenter incident. Fallback logic incorrectly defaulted to a deprecated UPS endpoint also returning 503.",
        "resolution": "Updated carrier fallback chain. Added health checks before failover. Implemented exponential backoff with jitter on carrier retries.",
        "keywords": ["503", "carrier API", "cascading failure", "FedEx", "shipping"],
    },
    {
        "incident_id": "INC-2024-1014",
        "summary": "Optimistic locking failure on concurrent order updates",
        "root_cause": "Background reconciliation job ran simultaneously with user-initiated status updates, both modifying the same order rows.",
        "resolution": "Moved reconciliation to off-peak window. Added explicit lock acquisition order to prevent deadlocks. Increased default retry attempts on OptimisticLockException.",
        "keywords": ["optimistic locking", "race condition", "concurrent update", "ObjectOptimisticLocking"],
    },
    {
        "incident_id": "INC-2024-1108",
        "summary": "OutOfMemoryError in order enrichment batch",
        "root_cause": "Batch size grew without bound when downstream service was slow. Heap consumed by accumulated unprocessed items.",
        "resolution": "Added max batch size of 1000. Switched from List to Stream processing. Increased JVM heap from 2G to 4G as buffer.",
        "keywords": ["OutOfMemoryError", "heap", "batch processing", "OrderEnricher"],
    },
]

INDEX_BODY = {
    "settings": {
        "index.knn": True,
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "incident_id": {"type": "keyword"},
            "summary":     {"type": "text"},
            "root_cause":  {"type": "text"},
            "resolution":  {"type": "text"},
            "keywords":    {"type": "keyword"},
            "embedding": {
                "type": "knn_vector",
                "dimension": 1024,
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


async def main():
    print(f"🔌 Connecting to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    client = AsyncOpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False, verify_certs=False,
    )

    # Recreate index
    exists = await client.indices.exists(index=INDEX_NAME)
    if exists:
        print(f"Deleting existing index {INDEX_NAME}")
        await client.indices.delete(index=INDEX_NAME)
    await client.indices.create(index=INDEX_NAME, body=INDEX_BODY)
    print(f"✅ Created index {INDEX_NAME}")

    # Embed each incident
    texts = [
        f"{i['summary']}. Root cause: {i['root_cause']}. Resolution: {i['resolution']}"
        for i in INCIDENTS
    ]
    print(f"Generating embeddings for {len(texts)} incidents...")
    vectors = embed(texts)

    # Bulk index
    actions = []
    for inc, vec in zip(INCIDENTS, vectors):
        actions.append({
            "_op_type": "index",
            "_index": INDEX_NAME,
            "_id": inc["incident_id"],
            "_source": {**inc, "embedding": vec},
        })

    success, errors = await helpers.async_bulk(client, actions, refresh=True)
    print(f"✅ Indexed {success} incidents")
    if errors:
        print(f"⚠️  Errors: {errors}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
