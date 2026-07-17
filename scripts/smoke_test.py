"""
Smoke test — quick verification of every live service in the stack.

Covers: OpenSearch, Postgres, Redis, Ollama, MCP server, Orchestrator.
Kafka/ingestor and the k-NN/embeddings checks from the original POC are
gone — the current architecture reads directly from existing OpenSearch
indices and uses full-text (DQL/BM25) search only, not vectors.

Note: services/monitor/poller.py (Module 3) has no HTTP endpoint to check
here — it's a background polling loop, not a server. Verify it directly via
its own terminal output (see docs/OPERATIONS_RUNBOOK_V2.md).

Run from project root:
    python scripts/smoke_test.py
"""
import asyncio
import os
import sys
import httpx
import asyncpg
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


async def check_opensearch():
    try:
        client = AsyncOpenSearch(hosts=[{"host": "localhost", "port": 9200}],
                                  use_ssl=False, verify_certs=False)
        health = await client.cluster.health()
        ok(f"OpenSearch reachable. Status: {health['status']}, Nodes: {health['number_of_nodes']}")

        try:
            indices = await client.cat.indices(format="json")
            user_indices = {i["index"]: i.get("docs.count", "0") for i in indices if not i["index"].startswith(".")}
            if user_indices:
                ok(f"Indices: {user_indices}")
            else:
                warn("No indices yet — run scripts/seed_incidents.py and/or generate_logs_opensearch.py")
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
        warn(f"Postgres: {e} (optional — only needed if get_order_status is used)")
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
        fail(f"Redis: {e} (required — Module 3's poller checkpoints/dedup depend on this)")
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
                ok(f"MCP server reachable. Tools ({len(tools)}): {tools}")
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
            if resp.status_code != 200:
                fail(f"Orchestrator returned {resp.status_code}")
                return False
            ok("Orchestrator reachable.")

            try:
                proj_resp = await client.get("http://localhost:8000/api/v1/projects")
                projects = [p["project_id"] for p in proj_resp.json().get("projects", [])]
                ok(f"Projects configured: {projects}")
            except Exception:
                warn("Could not fetch /api/v1/projects")

            return True
    except Exception as e:
        warn(f"Orchestrator: {e} (not running yet?)")
        return False


async def main():
    print("\n=== Infrastructure ===")
    results = []
    results.append(await check_opensearch())
    results.append(await check_postgres())
    results.append(await check_redis())

    print("\n=== Local LLM ===")
    results.append(await check_ollama())

    print("\n=== Application services ===")
    results.append(await check_mcp_server())
    results.append(await check_orchestrator())

    print("\n=== Not checked here ===")
    warn("services/monitor/poller.py (Module 3) has no HTTP endpoint — check its own terminal log output directly.")

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
