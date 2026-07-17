"""
=============================================================================
Mock Log Generator — Direct-to-OpenSearch, Real Production Schema
=============================================================================

Reuses the failure scenarios from generate_logs.py (a shared scenario-data
library, no transport of its own) and writes them directly into OpenSearch
using the real production field schema (Fluentbit-shipped Spring Boot logs):
@timestamp, env, msg, log_message, level, logger, instance, threadId,
loggingId, exception, requestURI, duration, filePath, ip, node.privateip,
msgSize, standards.

This exists ONLY so Module 1 (production-index retrieval) can be tested
locally before pointing at a real client index. In production this script
is never run — logs already land in OpenSearch via the existing pipeline.
The target index is intentionally NOT pre-created with an explicit mapping
here, to mirror the fact that the GenAI layer doesn't own that index's
lifecycle in production; OpenSearch's dynamic "strings" template creates
both a `text` field and a `.keyword` sub-field for loggingId, level, etc.,
which is what analyze_order_logs's term filter on `loggingId.keyword` relies on.

Usage:
    python generate_logs_opensearch.py --transactions 100 \\
        --index fluentbit-csg-gr2v_app_launchpad-alias
"""

import argparse
import asyncio
import random
from datetime import datetime, timedelta, timezone

from opensearchpy import AsyncOpenSearch, helpers

from generate_logs import LOCATIONS, ORDER_RANGE, pick_scenario


OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
DEFAULT_INDEX = "fluentbit-csg-gr2v_app_launchpad-alias"


def _to_prod_schema(poc_log: dict) -> dict:
    """Map the POC's mock-log shape onto the real Fluentbit field schema.
    `msg` and `log_message` are populated identically since it's unconfirmed
    which one is authoritative in the real index — verify against production
    tonight and drop whichever is redundant."""
    text = poc_log["message"]
    service = poc_log["service"]
    return {
        "@timestamp": poc_log["timestamp"],
        "env": poc_log.get("environment", "dev"),
        "msg": text,
        "log_message": text,
        "level": poc_log["level"],
        "logger": service,
        "instance": poc_log.get("host", "app-01"),
        "threadId": f"thread-{random.randint(1, 50)}",
        "loggingId": poc_log["trace_id"],
        "exception": poc_log.get("exception", ""),
        "requestURI": f"/api/orders/{poc_log['order_no']}",
        "duration": random.randint(10, 500),
        "filePath": f"com/example/{service.replace('-', '')}/Handler.java",
        "ip": f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "node.privateip": f"10.1.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "msgSize": len(text),
        "standards": "N/A",
    }


async def produce(client: AsyncOpenSearch, index: str, num_transactions: int) -> int:
    total = 0
    actions = []

    for i in range(num_transactions):
        order_id = random.randint(*ORDER_RANGE)
        order_no = f"ORD-{order_id:05d}"
        location_no = random.choice(LOCATIONS)
        customer_email = f"customer{order_id}@example.com"
        t0 = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 300))

        scenario_fn = pick_scenario()
        poc_logs = scenario_fn(order_no, location_no, customer_email, t0)

        for poc_log in poc_logs:
            actions.append({
                "_op_type": "index",
                "_index": index,
                "_source": _to_prod_schema(poc_log),
            })
            total += 1

        if i % 10 == 0:
            name = scenario_fn.__name__.replace("scenario_", "")
            print(f"[{i + 1}/{num_transactions}] {order_no} → {name} ({len(poc_logs)} logs)")

    success, errors = await helpers.async_bulk(client, actions, refresh=True)
    print(f"\n✅ Done. Indexed {success} logs into '{index}'.")
    if errors:
        print(f"⚠️  {len(errors)} errors: {errors[:3]}")
    return total


async def main():
    parser = argparse.ArgumentParser(description="Seed OpenSearch with production-schema mock logs")
    parser.add_argument("--transactions", type=int, default=100)
    parser.add_argument("--index", type=str, default=DEFAULT_INDEX)
    parser.add_argument("--host", type=str, default=OPENSEARCH_HOST)
    parser.add_argument("--port", type=int, default=OPENSEARCH_PORT)
    args = parser.parse_args()

    client = AsyncOpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False, verify_certs=False,
    )
    print(f"🔌 Connecting to OpenSearch at {args.host}:{args.port}")
    print(f"   Target index: {args.index} (dynamically mapped, not pre-created — matches real prod behavior)\n")

    await produce(client, args.index, args.transactions)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
