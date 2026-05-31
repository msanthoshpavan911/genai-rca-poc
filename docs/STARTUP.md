# Startup Guide

## Startup Files

There are two startup files at the project root:

| File | Platform | Description |
|------|----------|-------------|
| `run.ps1` | Windows PowerShell | Main startup script — all commands live here |
| `run.bat` | Windows Command Prompt | Thin wrapper that calls `run.ps1` |
| `Makefile` | Linux / macOS | Equivalent of `run.ps1` using `make` |

> On Windows, use `.\run.ps1` or `run.bat`. On Linux/macOS, use `make`.

---

## First-Time Setup (run in order)

### Step 1 — Start infrastructure
```powershell
.\run.ps1 up
```
Starts Docker containers: Kafka, OpenSearch, Postgres, Redis. Waits ~25 seconds for them to become healthy.

### Step 2 — Install Python dependencies
```powershell
.\run.ps1 install
```
Runs `pip install -r requirements.txt` for all 4 services (log-generator, ingestor, mcp-server, orchestrator).

### Step 3 — Download LLM models
```powershell
.\run.ps1 ollama-pull
```
Downloads `qwen2.5:7b` (~4.7 GB) and `qwen2.5:3b` (~2 GB) via Ollama. One-time download.

### Step 4 — Seed historical incidents
```powershell
.\run.ps1 seed-incidents
```
Loads past RCA incidents into OpenSearch. Used by the `find_similar_incidents` tool.

### Step 5 — Generate mock log data
```powershell
.\run.ps1 generate-logs
```
Produces 100 mock order transactions and pushes them into Kafka.

---

## Running the Services

Each service needs its own terminal window:

**Terminal 1 — Ingestor** (Kafka → OpenSearch)
```powershell
.\run.ps1 ingest
```

**Terminal 2 — MCP Server** (port 8001)
```powershell
.\run.ps1 mcp
```

**Terminal 3 — Orchestrator / LangGraph Agent** (port 8000)
```powershell
.\run.ps1 orch
```

**Terminal 4 — React UI** (port 3000, optional)
```powershell
.\run.ps1 ui
```

---

## All Available Commands

| Command | What it does |
|---------|-------------|
| `.\run.ps1 up` | Start Docker infrastructure |
| `.\run.ps1 down` | Stop Docker containers (data is preserved) |
| `.\run.ps1 nuke` | Stop containers and delete all data volumes |
| `.\run.ps1 smoke` | Verify all services are healthy |
| `.\run.ps1 install` | Install Python deps for all services |
| `.\run.ps1 ollama-pull` | Download Ollama LLM models |
| `.\run.ps1 seed-incidents` | Seed historical incidents into OpenSearch |
| `.\run.ps1 generate-logs` | Generate 100 mock transactions into Kafka |
| `.\run.ps1 generate-burst` | Generate 1000 mock transactions (burst mode) |
| `.\run.ps1 ingest` | Run Kafka → vector ingestor |
| `.\run.ps1 mcp` | Run MCP server on port 8001 |
| `.\run.ps1 orch` | Run orchestrator on port 8000 |
| `.\run.ps1 ui` | Serve React UI on port 3000 |
| `.\run.ps1 demo` | Quick test — asks "Why did ORD-00005 fail?" |
| `.\run.ps1 help` | Show all commands |

---

## Service Ports

| Service | URL |
|---------|-----|
| Orchestrator (LangGraph API) | http://localhost:8000 |
| MCP Server | http://localhost:8001 |
| React UI | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| OpenSearch | http://localhost:9200 |
| OpenSearch Dashboards | http://localhost:5601 |
| Postgres | localhost:5432 |
| Redis | localhost:6379 |

---

## Quick Smoke Test

After all services are running, test with:
```powershell
.\run.ps1 demo
```
Sends `"Why did ORD-00005 fail?"` to the orchestrator and prints the RCA response.

Or call the API directly:
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/chat" `
  -Method Post `
  -ContentType "application/json" `
  -Body '{"message": "Why did ORD-00005 fail?", "session_id": "test"}'
```

---

## Stopping Everything

```powershell
.\run.ps1 down       # stop containers, keep data
.\run.ps1 nuke       # stop containers, delete all data
```

---

## What Gets Preloaded on Deployment

Deployment happens in three layers. Here is exactly what each layer loads into memory before the first user request arrives.

---

### Layer 1 — Docker Infrastructure (`.\run.ps1 up`)

| Container | What it preloads at startup |
|-----------|----------------------------|
| **PostgreSQL** | Starts DB engine, runs `init.sql` on first boot (creates orders / locations / payments / shipments tables) |
| **Redis** | Loads in-memory store, ready for session operations |
| **OpenSearch** | Starts JVM with 1 GB heap, initializes cluster state (~30-60 seconds) |
| **Kafka** | Formats KRaft log dirs, elects controller, ready to produce/consume |
| **Kafka UI** | Waits for Kafka health, then starts web dashboard |
| **OpenSearch Dashboards** | Waits for OpenSearch health, then starts UI |

> PostgreSQL only runs `init.sql` on the very first boot. After that it rehydrates from the persisted volume.

---

### Layer 2 — MCP Server (`.\run.ps1 mcp`)

This is the most significant preloading in the project. The `lifespan()` function in `services/mcp-server/server.py` runs before the first request is served:

```python
app.state.os   = AsyncOpenSearch(...)          # OpenSearch client created
app.state.pool = await asyncpg.create_pool(    # Postgres pool opened
    POSTGRES_DSN, min_size=2, max_size=10      # 2 connections opened immediately
)
embed_one("warmup")                            # Embedding model loaded into RAM
```

| What | Details |
|------|---------|
| **OpenSearch async client** | A persistent HTTP client to OpenSearch, shared across all 3 tool endpoints |
| **Postgres connection pool** | `min_size=2` means 2 real DB connections are opened at startup and kept alive — no connection overhead per request |
| **BGE embedding model** (`BAAI/bge-large-en-v1.5`) | ~1.3 GB SentenceTransformer model loaded into RAM via `embed_one("warmup")`. First run downloads it; subsequent runs load from cache |

---

### Layer 3 — Orchestrator (`.\run.ps1 orch`)

| What | When | Details |
|------|------|---------|
| **LangGraph graph compiled** | Module load (before server starts) | `agent = build_graph()` — all 9 nodes, edges, and routing logic compiled into a runnable object in memory |
| **Redis client connected** | App startup event | `redis_client.ping()` verifies the connection is live |

---

### What is NOT Preloaded (Lazy — loaded on first request)

| Component | When it loads |
|-----------|--------------|
| **Router LLM** (`qwen2.5:3b`) | First request where no order number is found in the message |
| **Synthesis LLM** (`qwen2.5:7b`) | First `ORDER_INQUIRY` request, inside the `synthesize` node |
| **MCP tool HTTP calls** | Every request — each node opens a fresh `httpx` connection to the MCP server on demand |

---

### Full Startup Timeline

```
.\run.ps1 up
  ├── PostgreSQL   → runs init.sql (tables created on first boot)
  ├── Redis        → ready
  ├── OpenSearch   → JVM warms up (~30-60s)
  ├── Kafka        → controller elected, ready
  ├── Kafka UI     → waits for Kafka, then starts
  └── OpenSearch Dashboards → waits for OpenSearch, then starts

.\run.ps1 mcp   (MCP Server)
  ├── OpenSearch async client created     ← preloaded
  ├── Postgres connection pool opened     ← preloaded (2 connections)
  └── BGE embedding model loaded into RAM ← preloaded (~1.3 GB)

.\run.ps1 orch  (Orchestrator)
  ├── LangGraph graph compiled → agent ready  ← preloaded
  └── Redis client connected                  ← preloaded

First user message
  ├── Router LLM instantiated (if no order number in message)  ← lazy
  └── Synthesis LLM instantiated (ORDER_INQUIRY path only)     ← lazy
```

---

### Warm vs Cold on First Request

| Component | Warm at startup? | Cold start penalty |
|-----------|-----------------|-------------------|
| Postgres queries | Yes (pool open) | None |
| OpenSearch queries | Yes (client ready) | None |
| Embedding (`find_similar`) | Yes (`warmup` call) | None |
| LangGraph routing | Yes (compiled) | None |
| Redis session ops | Yes (connected) | None |
| Router LLM | No | First Ollama HTTP round-trip |
| Synthesis LLM | No | First Ollama HTTP round-trip |
