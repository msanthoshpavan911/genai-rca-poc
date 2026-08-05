"""
OpenSearch connectivity checker.

Connects to OpenSearch, verifies the cluster is reachable, and lists all
available indices with their document counts.

Usage:
    python scripts/check_opensearch.py
"""

import asyncio
import os

from opensearchpy import AsyncOpenSearch


OPENSEARCH_HOST     = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT     = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
OPENSEARCH_USE_SSL  = os.getenv("OPENSEARCH_USE_SSL", "false").lower() in ("true", "1", "yes")
OPENSEARCH_CA_CERTS = os.getenv("OPENSEARCH_CA_CERTS", "")


def _build_client() -> AsyncOpenSearch:
    kwargs = {
        "hosts": [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        "use_ssl": OPENSEARCH_USE_SSL,
        "verify_certs": False,
    }
    if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
        kwargs["http_auth"] = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    if OPENSEARCH_CA_CERTS:
        kwargs["ca_certs"] = OPENSEARCH_CA_CERTS
    return AsyncOpenSearch(**kwargs)


async def main():
    client = _build_client()

    auth_label = f"{OPENSEARCH_USER}@" if OPENSEARCH_USER else ""
    ssl_label  = " [SSL]" if OPENSEARCH_USE_SSL else ""
    print(f"\n[INFO] Connecting to OpenSearch at {auth_label}{OPENSEARCH_HOST}:{OPENSEARCH_PORT}{ssl_label} ...\n")

    # 1. Cluster health
    try:
        health = await client.cluster.health()
        status = health.get("status", "unknown").upper()
        print("=" * 70)
        print(f" [STATUS] CLUSTER HEALTH: {status}")
        print("=" * 70)
        print(f"  Cluster Name : {health.get('cluster_name')}")
        print(f"  Nodes        : {health.get('number_of_nodes')} total, {health.get('number_of_data_nodes')} data")
        print(f"  Active Shards: {health.get('active_primary_shards')} primary, {health.get('active_shards')} total")
    except Exception as e:
        print(f" [FAIL] COULD NOT CONNECT TO OPENSEARCH: {e}")
        await client.close()
        return

    # 2. Engine info
    try:
        info = await client.info()
        version = info.get("version", {}).get("number", "unknown")
        print(f"  Engine       : OpenSearch v{version}")
    except Exception:
        pass

    print()

    # 3. List indices
    try:
        cat_indices = await client.cat.indices(format="json", s="index")
        if cat_indices:
            print("=" * 70)
            print(f" [INFO] INDICES LIST ({len(cat_indices)} total)")
            print("=" * 70)
            print(f" {'HEALTH':<8} {'STATUS':<8} {'INDEX NAME':<40} {'DOCS':>8} {'SIZE':>10}")
            print("-" * 75)
            for idx in cat_indices:
                h = (idx.get("health") or "").upper()
                st = idx.get("status") or ""
                name = idx.get("index") or ""
                docs = idx.get("docs.count") or "0"
                sz = idx.get("store.size") or "0b"
                print(f" {h:<8} {st:<8} {name:<40} {docs:>8} {sz:>10}")
            print("=" * 75)
        else:
            print(" [INFO] OpenSearch is running but has no indices yet.")
    except Exception as e:
        print(f" [FAIL] Could not list indices: {e}")

    await client.close()
    print("\n[OK] Check complete.\n")


if __name__ == "__main__":
    asyncio.run(main())
