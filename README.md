# GenAI Log Analysis & RCA Chatbot — Local POC

A complete, **zero-cost, runs-on-your-laptop** implementation of the architecture
we designed. Every component is open-source and local. The Claude API is
swapped for **Ollama** running locally; everything else is production-identical.

---

## What This POC Demonstrates

1. **Spring Boot → Kafka → OpenSearch** pipeline (mocked with a Python log generator)
2. **Parallel Python ingestor** consuming Kafka with its own consumer group
3. **Full-text log search** (DQL/BM25 on OpenSearch — no embeddings, no vector index)
4. **MCP server** with 3 tools (analyze_order_logs, get_order_status, find_similar_incidents)
5. **LangGraph agent** that classifies intent and orchestrates tool calls
6. **Local LLM** (Ollama with Qwen 2.5) for RCA synthesis
7. **React chatbot UI** with a polished demo experience

Realistic mock scenarios baked in: DB connection failures, NullPointerException,
payment gateway timeouts, circuit breaker trips, inventory mismatches, fraud
blocks, cascading 503s, race conditions, OOM, JSON parse errors.

---

## System Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB | **16 GB** |
| Disk | 20 GB | 50 GB |
| GPU | None (CPU works) | Any, or Apple Silicon |
| OS | macOS / Linux / Windows | macOS / Linux |

If you're on an M1+ Mac with 16 GB RAM, you're golden.

---

## Prerequisites

Install once:

```bash
# Docker Desktop:        https://www.docker.com/products/docker-desktop
# Python 3.11+:          https://www.python.org/downloads/
# Node.js (for UI dev):  https://nodejs.org/   (optional — UI works without it)
# Ollama:                https://ollama.com
```

After installing Ollama, pull the LLMs (one-time, ~6 GB download):

```bash
ollama pull qwen2.5:7b   # for RCA synthesis (4.7 GB)
ollama pull qwen2.5:3b   # for intent routing (2 GB)
```

---

## Windows Users — IMPORTANT

The `make` command doesn't exist on Windows by default. You have two equivalent options:

### Option A: Use the PowerShell script (recommended)
Every `make <target>` command in this README has an equivalent:

```powershell
.\run.ps1 up                 # instead of: make up
.\run.ps1 install            # instead of: make install
.\run.ps1 smoke              # instead of: make smoke
.\run.ps1 generate-logs      # instead of: make generate-logs
# ... etc.
```

If you get **"running scripts is disabled on this system"**, run this once in an admin PowerShell:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Option B: Use the batch file wrapper
From regular Command Prompt:
```cmd
run.bat up
run.bat install
run.bat smoke
```

### Option C: Install `make` on Windows
If you really want `make`:
```powershell
# With Chocolatey:
choco install make

# With Scoop:
scoop install make
```

Mac/Linux users can keep using `make` as documented below.

---

## Quick Start (10 minutes)

### Mac/Linux
```bash
make up                  # 1. Start infrastructure
make install             # 2. Install Python deps
make smoke               # 3. Verify everything is healthy
make seed-incidents      # 4. Seed historical incidents
make generate-logs       # 5. Generate 100 mock transactions
```

### Windows (PowerShell)
```powershell
.\run.ps1 up
.\run.ps1 install
.\run.ps1 smoke
.\run.ps1 seed-incidents
.\run.ps1 generate-logs
```

Now open **3 separate terminals**:

### Mac/Linux
```bash
# Terminal 1: Start the log ingestor
make ingest

# Terminal 2: Start the MCP server
make mcp

# Terminal 3: Start the orchestrator
make orch
```

### Windows (PowerShell)
```powershell
# Terminal 1: Start the log ingestor
.\run.ps1 ingest

# Terminal 2: Start the MCP server
.\run.ps1 mcp

# Terminal 3: Start the orchestrator
.\run.ps1 orch
```

Then in a 4th terminal:

```bash
# Mac/Linux
make demo                  # one-shot curl test
make ui                    # OR serve the React UI at http://localhost:3000/index.html
```

```powershell
# Windows
.\run.ps1 demo
.\run.ps1 ui
```

---

## Verifying Each Layer Works

### Check Kafka has messages
Open Kafka UI: http://localhost:8080
→ Navigate to Topics → `app-logs` → Messages

### Check OpenSearch has indexed log chunks
```bash
curl 'http://localhost:9200/logs-vectors-current/_count'
# Expect: {"count": ~50-200, ...}

curl 'http://localhost:9200/logs-vectors-current/_search?size=1&pretty'
# Should show a document with a composed_text / message field (plain text, no embedding vector)
```

Or use OpenSearch Dashboards: http://localhost:5601

### Check Postgres has orders
```bash
docker exec -it poc-postgres psql -U postgres -d orders \
  -c "SELECT order_no, status, failed_step FROM orders WHERE status='FAILED' LIMIT 5;"
```

### Test the MCP tools directly
```bash
# Get order status
curl -X POST http://localhost:8001/tools/get_order_status \
  -H "Content-Type: application/json" \
  -d '{"order_no": "ORD-00005"}' | python -m json.tool

# Analyze logs for an order
curl -X POST http://localhost:8001/tools/analyze_order_logs \
  -H "Content-Type: application/json" \
  -d '{"order_no": "ORD-00005", "additional_context": "payment failure", "top_k": 5}' \
  | python -m json.tool
```

### Test the end-to-end RCA
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Why did ORD-00005 fail?", "session_id": "demo"}' \
  | python -m json.tool
```

---

## Demo Scenarios

The mock log generator produces these realistic failure scenarios. Try asking
the chatbot about each:

| Question | Expected Behavior |
|---|---|
| "Why did ORD-00005 fail?" | Random failure scenario — payment, DB, NPE, etc. |
| "What happened with ORD-00010?" | Different failure path |
| "What is the status of ORD-00100?" | Status-only response (no log analysis) |
| "Why was ORD-00015 blocked?" | Likely fraud check scenario |
| "Hello, what can you do?" | General question response |
| "Help me" | Clarification request |

Order numbers from `ORD-00001` to `ORD-00200` are seeded with realistic data.

---

## How RAG Works Here

This is a Retrieval-Augmented Generation pipeline using **full-text search, not vector search**:

1. **Retrieve** — three tools gather evidence before any generation:
   - `get_order_status` — SQL JOIN on Postgres (orders/payments/shipments)
   - `analyze_order_logs` — OpenSearch DQL, hard `term` filter on `order_no`, boosted by `has_error`
   - `find_similar_incidents` — OpenSearch DQL `multi_match` (BM25) across past RCA summaries
2. **Augment** — retrieved evidence is labeled (timestamps, services, error lines) and injected into the synthesis prompt.
3. **Generate** — the LLM (`qwen2.5:7b` via Ollama) writes the RCA using *only* the injected evidence, and must say "insufficient evidence" rather than speculate.

No embedding model or vector index is used — log error text (`HikariCP`, `NullPointerException`, etc.) has distinctive enough vocabulary that BM25 term matching outperforms semantic similarity here, and it avoids ~3 GB of extra dependencies. If a query's logs contain no errors, the flow skips the LLM entirely and returns a deterministic step summary instead — zero hallucination risk on the happy path.

See `docs/PROJECT_DOCUMENTATION.md` for the full breakdown.

---

## Architecture Recap

```
┌────────────────────────────────────────────────────────────────┐
│  Mock Log Generator      Postgres (orders, payments, etc.)     │
│  (mimics Spring Boot)         ↑                                │
│         │                     │                                │
│         ▼                     │                                │
│      Kafka                    │                                │
│   (app-logs)                  │                                │
│    │       │                  │                                │
│    │       └──► Ingestor (Python) ──► OpenSearch (DQL/BM25)    │
│    │                                  (logs-vectors-*)          │
│    │                                       │                    │
│    └──► (Logstash — skipped for POC)       │                   │
│                                            │                    │
│                          MCP Server (3 tools)                   │
│                                  │                              │
│                                  ▼                              │
│                          LangGraph Agent                        │
│                          + Ollama (local LLM)                   │
│                                  │                              │
│                                  ▼                              │
│                          React Chatbot UI                       │
└────────────────────────────────────────────────────────────────┘
```

---

## Layout

```
genai-rca-poc/
├── infra/
│   └── docker-compose.yml          # All infrastructure
├── scripts/
│   ├── init.sql                    # Postgres seed data
│   ├── seed_incidents.py           # Historical RCAs into OpenSearch
│   └── smoke_test.py               # Verify all services
├── services/
│   ├── log-generator/
│   │   ├── generate_logs.py        # Mock Spring Boot logs → Kafka
│   │   └── requirements.txt
│   ├── ingestor/
│   │   ├── ingestor.py             # Kafka → OpenSearch (DQL full-text)
│   │   └── requirements.txt
│   ├── mcp-server/
│   │   ├── server.py               # 3 tools as HTTP endpoints
│   │   └── requirements.txt
│   ├── orchestrator/
│   │   ├── main.py                 # FastAPI + LangGraph + Ollama
│   │   └── requirements.txt
│   └── ui/
│       └── index.html              # Single-file React chatbot
├── Makefile                        # Convenience commands
└── README.md                       # This file
```

---

## Troubleshooting

### Windows: "running scripts is disabled on this system"
PowerShell blocks scripts by default. Fix with (admin PowerShell, one-time):
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
Then close and reopen PowerShell.

### Windows: "docker compose" not recognized
Make sure Docker Desktop is installed AND running (check the system tray icon).
If you have older Docker installed via Toolbox, use `docker-compose` (with hyphen) instead.

### Windows: Python script seems to hang or use the wrong Python
Windows has the `py` launcher which picks up Python 3 correctly. Verify with:
```powershell
py --version
```
If `py` isn't found, the script falls back to `python`. Make sure that points to Python 3.11+:
```powershell
python --version
```

### Windows: Path issues with backslashes
The `run.ps1` script uses native PowerShell paths and handles this automatically. If you're running individual Python commands manually, use forward slashes in arguments to be safe.

### Docker containers won't start
```bash
docker compose -f infra/docker-compose.yml logs
# Common: not enough RAM. Stop other apps. Or reduce OpenSearch heap to 512m in docker-compose.yml
```

### `make smoke` shows Kafka unreachable
Wait 30 seconds after `make up` — Kafka takes time to be ready.

### Ollama "model not found"
```bash
ollama list  # check what you have
ollama pull qwen2.5:7b
```

### Orchestrator times out on first query
First LLM call loads the model into RAM (~5 GB). Subsequent calls are fast.
Watch the Ollama logs: `journalctl -u ollama -f` or check Ollama's terminal output.

### OpenSearch returns 0 results from analyze_order_logs
1. Verify ingestor has run successfully and `_count` > 0 on the index
2. Refresh the index: `curl -X POST http://localhost:9200/logs-vectors-current/_refresh`
3. Confirm the `order_no` you're querying actually exists in the index (see the aggregation query in `docs/PROJECT_DOCUMENTATION.md`)

### React UI shows "Error: failed to fetch"
The orchestrator probably isn't running on port 8000. Check with `make smoke`.

### Want to start completely fresh
```bash
make nuke              # delete all Docker volumes
make up                # rebuild
make install           # if you skipped this earlier
make seed-incidents
make generate-logs
```

---

## Stretch Goals (Optional Enhancements)

Once the basic POC works, try:

1. **True MCP protocol** — convert `server.py` to use the official `mcp` Python SDK with stdio transport.
2. **Streaming responses** — wire the orchestrator's `/api/v1/chat/stream` endpoint to the React UI for token-by-token display.
3. **Langfuse** — add `langfuse:` to docker-compose and instrument every LangGraph node for observability.
4. **Evaluation harness** — build a golden test set + Ragas evaluation script.
5. **Production model swap** — change `ChatOllama` to `ChatAnthropic` in `orchestrator/main.py` to use real Claude Sonnet 4.5. One-line change.

---

## Cost Tracker

| Component | Cost |
|---|---|
| Docker | $0 |
| Kafka, OpenSearch, Postgres, Redis | $0 |
| Ollama + Qwen 2.5 | $0 |
| LangGraph, FastAPI, React | $0 |
| **Total** | **$0** |

The only thing you spend is your time and a bit of electricity.

---

## Going from POC to Production

When you're ready for the real client deployment:

| Local POC | Production |
|---|---|
| Single-node OpenSearch | Multi-node cluster, 3 shards + 1 replica |
| Mock log generator | Real Spring Boot logs via Kafka |
| Skip Logstash | Existing Logstash flow continues |
| Local Postgres | Order DB read-only replica |
| Ollama + Qwen 2.5 | Claude Sonnet 4.5 (1-line code change) |
| HTTP "MCP" endpoints | Official MCP protocol |
| Single-file React | Vite + assistant-ui + auth |
| No observability | LangSmith / Langfuse |

The architecture is identical. Components are interchangeable.

## To run in powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

## Stop service at 8080
Stop-Service -Name "Jenkins"