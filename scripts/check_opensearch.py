"""
OpenSearch connectivity checker.

Connects to OpenSearch, verifies the cluster is reachable, and lists all
available indices with their document counts and sizes.

Usage:
    python scripts/check_opensearch.py

Environment variables:
    OPENSEARCH_HOST      Host (default: localhost)
    OPENSEARCH_PORT      Port (default: 9200)
    OPENSEARCH_USER      Username for basic auth (default: none)
    OPENSEARCH_PASSWORD  Password for basic auth (default: none)
    OPENSEARCH_USE_SSL   Set to "true" to enable TLS (default: false)
    OPENSEARCH_CA_CERTS  Path to CA certificate file (optional, for self-signed certs)

Examples:
    # Local POC (no auth)
    python scripts/check_opensearch.py

    # Secured / enterprise
    OPENSEARCH_HOST=prod-host OPENSEARCH_USER=admin OPENSEARCH_PASSWORD=secret OPENSEARCH_USE_SSL=true python scripts/check_opensearch.py
"""

import asyncio
import os

from opensearchpy import AsyncOpenSearch


OPENSEARCH_HOST     = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT     = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
OPENSEARCH_USE_SSL  = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"
OPENSEARCH_CA_CERTS = os.getenv("OPENSEARCH_CA_CERTS", "")


def _build_client() -> AsyncOpenSearch:
    kwargs = {
        "hosts": [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        "use_ssl": OPENSEARCH_USE_SSL,
        "verify_certs": OPENSEARCH_USE_SSL,   # only verify when SSL is on
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
    print(f"\n🔌 Connecting to OpenSearch at {auth_label}{OPENSEARCH_HOST}:{OPENSEARCH_PORT}{ssl_label} ...\n")


    # ── 1. Cluster health ───────────────────────────────────────────────────
    try:
        health = await client.cluster.health()
        status = health.get("status", "unknown")
        icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(status, "⚪")
        print(f"{icon} Cluster health : {status.upper()}")
        print(f"   Cluster name  : {health.get('cluster_name')}")
        print(f"   Nodes         : {health.get('number_of_nodes')}")
        print(f"   Active shards : {health.get('active_shards')}")
    except Exception as e:
        print(f"❌ Could not reach OpenSearch: {e}")
        await client.close()
        return

    # ── 2. Cluster info (version) ───────────────────────────────────────────
    try:
        info = await client.info()
        version = info.get("version", {}).get("number", "unknown")
        print(f"   Version       : {version}\n")
    except Exception:
        print()

    # ── 3. List all indices ─────────────────────────────────────────────────
    try:
        # cat/indices returns a list of dicts when format=json
        indices = await client.cat.indices(
            params={"format": "json", "s": "index", "h": "index,status,health,docs.count,store.size,pri,rep"}
        )

        if not indices:
            print("ℹ️  No indices found.")
        else:
            print(f"{'INDEX':<40} {'STATUS':<10} {'HEALTH':<8} {'DOCS':>8} {'SIZE':>10}  {'PRI'}/{{'REP'}}")
            print("-" * 90)
            for idx in indices:
                health_icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(idx.get("health", ""), "⚪")
                print(
                    f"{idx.get('index', ''):<40} "
                    f"{idx.get('status', ''):<10} "
                    f"{health_icon} {idx.get('health', ''):<6} "
                    f"{idx.get('docs.count', '0'):>8} "
                    f"{idx.get('store.size', 'N/A'):>10}  "
                    f"{idx.get('pri', '?')}/{idx.get('rep', '?')}"
                )
            print(f"\n✅ Total indices: {len(indices)}")
    except Exception as e:
        print(f"❌ Failed to list indices: {e}")

    # ── 4. Quick query — document count per POC index ───────────────────────
    poc_indices = ["logs-vectors-current", "incidents-historical"]
    print("\n── POC Index Details ──────────────────────────────────────────────────\n")
    for index_name in poc_indices:
        try:
            exists = await client.indices.exists(index=index_name)
            if not exists:
                print(f"  {index_name:<30} ⚠️  does not exist")
                continue

            count_resp = await client.count(index=index_name)
            count = count_resp.get("count", 0)

            mapping = await client.indices.get_mapping(index=index_name)
            fields = list(
                mapping.get(index_name, {})
                .get("mappings", {})
                .get("properties", {})
                .keys()
            )

            print(f"  {index_name}")
            print(f"    Documents : {count}")
            print(f"    Fields    : {', '.join(fields)}\n")
        except Exception as e:
            print(f"  {index_name:<30} ❌ {e}\n")

    await client.close()
    print("👋 Done.\n")


if __name__ == "__main__":
    asyncio.run(main())
