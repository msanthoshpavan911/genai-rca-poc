"""
Smoke test — quick verification of every service in the stack.

Run from project root:
    python scripts/smoke_test.py
"""
import asyncio
import os
import sys
import httpx
import asyncpg
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from opensearchpy import AsyncOpenSearch

# Enable ANSI colors on Windows 10+ terminals
if sys.platform == "win32":
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# Disable colors if explicitly requested (e.g. for log capture) or if not a TTY
USE_COLORS = sys.stdout.isatty() and os.getenv("NO_COLOR") is None

if USE_COLORS:
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    RESET  = "\033[0m"
else:
    GREEN = RED = YELLOW = RESET = ""


def ok(msg):     print(f"{GREEN}✅ {msg}{RESET}")
def fail(msg):   print(f"{RED}❌ {msg}{RESET}")
def warn(msg):   print(f"{YELLOW}⚠️  {msg}{RESET}")


async def check_kafka():
    try:
        consumer = AIOKafkaConsumer(bootstrap_servers="localhost:9092")
        await consumer.start()
        topics = await consumer.topics()
        await consumer.stop()
        ok(f"Kafka reachable. Topics: {sorted(topics) or 'none yet'}")
        return True
    except Exception as e:
        fail(f"Kafka: {e}")
        return False


async def check_opensearch():
    try:
        client = AsyncOpenSearch(hosts=[{"host": "localhost", "port": 9200}],
                                  use_ssl=False, verify_certs=False)
        health = await client.cluster.health()
        ok(f"OpenSearch reachable. Status: {health['status']}, Nodes: {health['number_of_nodes']}")

        # Check if k-NN plugin is available
        try:
            plugins = await client.cat.plugins(format="json")
            knn = [p for p in plugins if "knn" in p.get("component", "").lower()]
            if knn:
                ok(f"k-NN plugin enabled: {knn[0].get('component')}")
            else:
                warn("k-NN plugin not found in cat/plugins (may still work in OpenSearch 2.x)")
        except Exception:
            pass

        # List existing indices
        try:
            indices = await client.cat.indices(format="json")
            poc_indices = [i["index"] for i in indices if not i["index"].startswith(".")]
            if poc_indices:
                ok(f"Indices: {poc_indices}")
            else:
                warn("No indices yet (will be created on first ingest)")
        except Exception:
            pass

        await client.close()
        return True
    except Exception as e:
        fail(f"OpenSearch: {e}")
        return False


async def check_postgres():
    try:
        conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/orders")
        orders_count = await conn.fetchval("SELECT COUNT(*) FROM orders")
        locations_count = await conn.fetchval("SELECT COUNT(*) FROM locations")
        failed_count = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE status='FAILED'")
        await conn.close()
        ok(f"Postgres reachable. Orders: {orders_count}, Locations: {locations_count}, Failed: {failed_count}")
        return True
    except Exception as e:
        fail(f"Postgres: {e}")
        return False


async def check_redis():
    try:
        import redis.asyncio as redis
        r = redis.from_url("redis://localhost:6379")
        await r.ping()
        await r.close()
        ok("Redis reachable.")
        return True
    except Exception as e:
        fail(f"Redis: {e}")
        return False


async def check_ollama():
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("http://localhost:11434/api/tags")
            data = resp.json()
            models = [m["name"] for m in data.get("models", [])]
            if not models:
                warn("Ollama is up but no models pulled. Run: ollama pull qwen2.5:7b")
                return False
            ok(f"Ollama reachable. Models: {models}")
            return True
    except Exception as e:
        fail(f"Ollama: {e}")
        return False


async def check_mcp_server():
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("http://localhost:8001/healthz")
            if resp.status_code == 200:
                tools_resp = await client.get("http://localhost:8001/tools")
                tools = [t["name"] for t in tools_resp.json().get("tools", [])]
                ok(f"MCP server reachable. Tools: {tools}")
                return True
            else:
                fail(f"MCP server returned {resp.status_code}")
                return False
    except Exception as e:
        warn(f"MCP server: {e} (not running yet?)")
        return False


async def check_orchestrator():
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("http://localhost:8000/healthz")
            if resp.status_code == 200:
                ok("Orchestrator reachable.")
                return True
            else:
                fail(f"Orchestrator returned {resp.status_code}")
                return False
    except Exception as e:
        warn(f"Orchestrator: {e} (not running yet?)")
        return False


async def main():
    print("\n=== Infrastructure ===")
    results = []
    results.append(await check_kafka())
    results.append(await check_opensearch())
    results.append(await check_postgres())
    results.append(await check_redis())

    print("\n=== Local LLM ===")
    results.append(await check_ollama())

    print("\n=== Application services ===")
    results.append(await check_mcp_server())
    results.append(await check_orchestrator())

    print("\n" + "=" * 40)
    passed = sum(1 for r in results if r)
    total = len(results)
    if passed == total:
        ok(f"All checks passed ({passed}/{total})")
    else:
        warn(f"{passed}/{total} checks passed")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
