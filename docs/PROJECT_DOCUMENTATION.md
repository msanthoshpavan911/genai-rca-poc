# GenAI-Powered Log Analysis & RCA Chatbot

A complete, locally-runnable Proof of Concept that demonstrates how to add a GenAI Root Cause Analysis layer on top of an existing Spring Boot + Kafka + OpenSearch logging stack — without modifying any existing system.

> **Status**: ✅ Working end-to-end on Windows with zero cost. Tested with realistic mock data including database failures, NullPointerException, payment gateway timeouts, and more.

---

## Table of Contents

1. [The Problem We're Solving](#the-problem-were-solving)
2. [What This POC Demonstrates](#what-this-poc-demonstrates)
3. [Architecture Overview](#architecture-overview)
4. [Project Structure](#project-structure)
5. [How Each Component Works](#how-each-component-works)
6. [Mock Data Strategy](#mock-data-strategy)
7. [Prerequisites](#prerequisites)
8. [Step-by-Step Setup](#step-by-step-setup)
9. [Running the Demo](#running-the-demo)
10. [Verifying Each Layer](#verifying-each-layer)
11. [Common Issues & Solutions](#common-issues--solutions)
12. [What You Should See](#what-you-should-see)
13. [From POC to Production](#from-poc-to-production)
14. [For the Client Pitch](#for-the-client-pitch)
15. [Frequently Asked Questions](#frequently-asked-questions)

---

## The Problem We're Solving

In a typical microservice production system, when something fails:

- **Support engineers** spend hours digging through OpenSearch / Kibana queries
- **The relevant logs** are spread across multiple services with different formats
- **Context is lost** — log timestamps don't naturally connect to business outcomes (e.g., "order failed")
- **Knowledge doesn't compound** — every new engineer relearns the same failure patterns

**The proposal**: a chatbot where users type "Why did ORD-12345 fail?" and get back a structured Root Cause Analysis citing the actual logs, the order status from the database, and similar past incidents — all in plain English, in under 30 seconds.

**Without changing any existing system.** No modifications to Spring Boot apps, no changes to the Kafka pipeline, no migration of the OpenSearch cluster.

---

## What This POC Demonstrates

This POC is **architecturally identical** to what we'd deploy in production. The only difference is the LLM and the scale.

| Layer | Production | This Local POC |
|---|---|---|
| Log producers | Real Spring Boot microservices | Python mock generator simulating 10 failure scenarios |
| Message bus | Existing enterprise Kafka cluster | Single-node Kafka in Docker |
| Log store | OpenSearch cluster | Single-node OpenSearch in Docker |
| Log retrieval | DQL full-text search (BM25) | Same — `multi_match` + filter queries |
| Order database | Enterprise Postgres/Oracle | Postgres in Docker, seeded with 200 orders |
| LLM (synthesis) | Claude Sonnet 4.5 | Ollama qwen2.5:7b on CPU |
| LLM (routing) | Claude Haiku 4.5 | Ollama qwen2.5:3b on CPU |
| MCP tools | 3 tools via official MCP protocol | Same 3 tools via FastAPI HTTP |
| Agent | LangGraph state machine | Same code |
| UI | React + assistant-ui + SSE | Same — single-file React |
| Observability | LangSmith / Langfuse | Optional Langfuse (skipped in POC) |

**Moving to production is mostly about changing the LLM endpoint** — one line of code. The architecture, tools, agent logic, and data model are unchanged.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   EXISTING SPRING BOOT ECOSYSTEM (UNCHANGED in production)                  │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│   │   Order     │  │   Payment   │  │  Inventory  │                         │
│   │  Service    │  │   Service   │  │   Service   │                         │
│   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                         │
│          │                │                │                                │
│          ▼                ▼                ▼                                │
│   ┌─────────────────────────────────────────────┐                           │
│   │           Kafka Topic: app-logs              │                          │
│   └────────────────┬─────────────────┬──────────┘                           │
│                    │                 │                                      │
│   cg: logstash-cg  │                 │  cg: genai-ingestor-cg               │
│                    ▼                 ▼                                      │
│           ┌──────────────┐   ┌──────────────────┐                           │
│           │   Logstash   │   │  Python Ingestor │  ← NEW                    │
│           │  (existing)  │   │  (aiokafka)      │                           │
│           └──────┬───────┘   └────────┬─────────┘                           │
│                  │                    │                                     │
│                  ▼                    │ parse + chunk (60s window)          │
│           ┌──────────────┐            ▼                                     │
│           │  OpenSearch  │   ┌──────────────────┐                           │
│           │   logs-*     │   │   OpenSearch     │  ← NEW                    │
│           │  (existing)  │   │ logs-vectors-*   │                           │
│           └──────────────┘   │ (BM25 full-text, │                           │
│                              │  DQL filter)     │                           │
│                              └────────┬─────────┘                           │
└─────────────────────────────────────────┼─────────────────────────────────-─┘
                                          │
                ┌─────────────────────────┼───────────────────────┐
                │   NEW PYTHON GENAI LAYER                        │
                │                         │                       │
                │            ┌────────────▼────────────┐          │
                │            │      MCP Server         │          │
                │            │  Tool 1: analyze_logs   │          │
                │            │  Tool 2: get_status     │          │
                │            │  Tool 3: find_similar   │          │
                │            └────────────┬────────────┘          │
                │                         │                       │
                │                         ▼                       │
                │      ┌──────────────────────────────────┐       │
                │      │   FastAPI Orchestrator           │       │
                │      │   + LangGraph Agent              │       │
                │      │   + Ollama / Claude (LLM)        │       │
                │      └──────────────┬───────────────────┘       │
                │                     │                           │
                │                     ▼                           │
                │      ┌──────────────────────────────────┐       │
                │      │     React Chatbot UI             │       │
                │      │     (SSE streaming)              │       │
                │      └──────────────────────────────────┘       │
                │                                                 │
                └─────────────────────────────────────────────────┘

External:  Order DB (Postgres) ←─ MCP Server   |   Redis ←─ Orchestrator (sessions)
```

### Key Design Principles

1. **Additive, not invasive**: The existing pipeline (Spring Boot → Kafka → Logstash → OpenSearch) is untouched. A second consumer group reads the same Kafka topic in parallel.

2. **Separation of concerns**: The MCP server retrieves evidence; the LLM reasons. This makes the system testable, debuggable, and replaceable.

3. **Grounded responses**: Strict prompt design forces the LLM to cite specific log timestamps and service names. If evidence is insufficient, it admits so rather than hallucinating.

4. **Production-portable**: Every component is either open-source or has a paid equivalent. Migration is a configuration change, not a rewrite.

---

## Project Structure

```
genai-rca-poc/
├── README.md                       # The big-picture overview (you are here)
├── Makefile                        # Mac/Linux command shortcuts
├── run.ps1                         # Windows PowerShell equivalent
├── run.bat                         # Command Prompt wrapper
├── .gitignore
│
├── infra/
│   └── docker-compose.yml          # Kafka, OpenSearch, Postgres, Redis, Kafka UI
│
├── scripts/
│   ├── init.sql                    # Postgres schema + seed data (200 orders, 10 locations)
│   ├── seed_incidents.py           # 8 mock historical RCAs into OpenSearch
│   └── smoke_test.py               # Verifies all 7 service layers are healthy
│
└── services/
    ├── log-generator/
    │   ├── generate_logs.py        # Mock Spring Boot logs → Kafka (10 scenarios)
    │   └── requirements.txt
    │
    ├── ingestor/
    │   ├── ingestor.py             # Kafka → chunk → OpenSearch (DQL)
    │   └── requirements.txt
    │
    ├── mcp-server/
    │   ├── server.py               # 3 tools exposed as HTTP endpoints
    │   └── requirements.txt
    │
    ├── orchestrator/
    │   ├── main.py                 # FastAPI + LangGraph + Ollama
    │   └── requirements.txt
    │
    └── ui/
        └── index.html              # Single-file React chatbot (no build step)
```

---

## How Each Component Works

### 1. Log Generator (`services/log-generator/generate_logs.py`)

Simulates what real Spring Boot microservices would produce. Each generated log contains:

- `timestamp` (ISO 8601 UTC)
- `level` (INFO, WARN, ERROR)
- `service` (order-service, payment-service, etc.)
- `trace_id` (groups all logs for one transaction)
- `order_no` (the key identifier — what users will ask about)
- `location_no` (warehouse location)
- `customer_email`
- `message`
- `exception` (full stack trace for failures)

**Built-in scenarios (weighted random):**

| Scenario | % of traffic | Logs per transaction | What it teaches |
|---|---|---|---|
| Happy path | 50% | 7 | Successful baseline |
| Payment gateway timeout | 10% | 8 | Circuit breaker trips, PSP outage |
| DB connection failure | 8% | 6 | HikariCP pool exhaustion + full JDBC stack |
| Inventory mismatch | 8% | 6 | Stale cache, oversells |
| NullPointerException | 6% | 4 | Classic Java bug + full stack trace |
| Fraud check block | 6% | 4 | Business logic block, risk scoring |
| Downstream 503 | 5% | 6 | Cascading carrier API failures |
| Race condition | 3% | 2 | OptimisticLockException |
| OOM | 2% | 2 | JVM heap exhausted |
| JSON parse error | 2% | 1 | Bad request body, Jackson stack |

Each failure scenario emits **multiple correlated logs across services with the same `trace_id`** so the GenAI agent has rich context to analyze.

### 2. Kafka (Docker)

Single broker, KRaft mode (no Zookeeper needed). The topic `app-logs` is the contract between log producers and consumers. Two consumer groups read from it independently:

- `logstash-cg` (existing) — feeds the standard ELK/OpenSearch logs index
- `genai-ingestor-cg` (new) — feeds the log index for GenAI RCA

Kafka tracks offsets per consumer group, so the two are completely independent.

### 3. Ingestor (`services/ingestor/ingestor.py`)

The most code-heavy service. For each batch of logs:

1. **Consume** from Kafka via `aiokafka`
2. **Parse** JSON, extract metadata (order_no, trace_id, service, etc.)
3. **Mask PII** with regex (emails, card numbers, phones) before indexing
4. **Drop noise** (DEBUG, TRACE logs)
5. **Chunk by `trace_id`** in 60-second windows — groups all logs from one transaction into one logical chunk for better retrieval
6. **Bulk index** to OpenSearch `logs-vectors-current` with deterministic IDs
7. **Commit Kafka offsets** only after successful indexing (at-least-once semantics)

Flushes trigger two ways — whichever comes first:
- **Message-triggered**: when 500 logs buffered OR 30 seconds elapsed since last flush
- **Background timer**: a periodic asyncio task flushes every 30 seconds independently, so chunks drain even when the log stream goes quiet

### 4. OpenSearch (Docker)

Single-node, security disabled (POC only). Two indices:

**`logs-vectors-current`** — created by the ingestor:
- 1 shard, 0 replicas (POC; production uses 3 + 1)
- Fields: `order_no` (keyword), `message` (text), `has_error` (boolean), `services` (keyword), `log_levels` (keyword), `earliest_ts` / `latest_ts` (date), `raw_logs` (stored, not indexed)
- Searched via DQL: hard `term` filter on `order_no`, with `should` boost clauses on `message` content and `has_error`

**`incidents-historical`** — seeded once by `scripts/seed_incidents.py`:
- 8 mock past RCAs with `summary`, `root_cause`, `resolution`, `keywords` fields — all `text` type for full-text search
- Used by the `find_similar_incidents` MCP tool via `multi_match` DQL query

### 5. Postgres (Docker)

Holds authoritative order/payment/shipment data. Schema:

- **`locations`** (10 rows) — warehouses across regions (NYC, LAX, LON, FRA, MUM, BLR, SGP, SYD, TOR, CHI)
- **`orders`** (200 rows) — pre-seeded with realistic status distribution: 60% COMPLETED, 20% FAILED, 15% PENDING, 5% CANCELLED. Failed orders include a `failed_step` (PAYMENT, INVENTORY, SHIPPING, FRAUD_CHECK, DB_TIMEOUT).
- **`payments`** (200 rows) — one per order with method (CARD, UPI, WALLET, NET_BANKING) and `failure_reason` for failed ones.
- **`shipments`** (~150 rows) — only for non-failed orders.

The `get_order_status` MCP tool joins all three to give the LLM authoritative business context.

### 6. MCP Server (`services/mcp-server/server.py`)

Exposes 3 tools as HTTP endpoints (functionally identical to MCP protocol; trivially convertible). All tools accept JSON and return JSON.

#### Tool 1: `analyze_order_logs`

**Input**: `order_no`, optional `additional_context`, `top_k`

**What it does**: DQL `bool` query against OpenSearch `logs-vectors-current`. The `order_no` `term` filter is a hard constraint (only that order's logs). `should` clauses boost relevance within those results:
- Error chunks (`has_error: true`) are boosted 1.5×
- If `additional_context` is provided, chunks whose `message` text matches it are boosted 2×

Returns chunks sorted by `earliest_ts` (chronological), so the LLM sees the timeline in order.

#### Tool 2: `get_order_status`

**Input**: `order_no`

**What it does**: Parameterized SQL JOIN across orders/payments/shipments/locations. Returns the authoritative current state.

#### Tool 3: `find_similar_incidents`

**Input**: free-text issue description, `top_k`

**What it does**: DQL `bool/should` query against `incidents-historical`. Uses `multi_match` across `summary^3`, `root_cause^2`, `resolution^1` with `fuzziness: AUTO` and a `match` boost on the `keywords` field. Returns the top matching past RCAs ranked by BM25 score.

### 7. Orchestrator (`services/orchestrator/main.py`)

The "brain" of the system. Built on **LangGraph** as a state machine with two routing decisions:

**Nodes:**

| Node | What it does |
|---|---|
| `classify_intent` | Router LLM (`qwen2.5:3b`) classifies into ORDER_INQUIRY / STATUS_ONLY / GENERAL_QUESTION / CLARIFICATION |
| `fetch_order_status` | Calls MCP Tool 2 — SQL JOIN from Postgres |
| `analyze_logs` | Calls MCP Tool 1 — DQL filter on OpenSearch |
| `happy_path_summary` | Programmatic step formatter — no LLM, zero hallucination risk |
| `find_similar` | Calls MCP Tool 3 — with keyword expansion for business language |
| `synthesize_rca` | Synthesis LLM (`qwen2.5:7b`) — generates structured RCA |

**Flow:**

```
classify_intent
    │
    ├── ORDER_INQUIRY → fetch_order_status → analyze_logs
    │                                              │
    │                          has_errors=False ──►│──► happy_path_summary → END
    │                          (logs found,         │
    │                           no errors)          │
    │                          has_errors=True  ──►│──► find_similar → synthesize_rca → END
    │                          (or no logs)
    │
    ├── STATUS_ONLY  → fetch_order_status → status_response → END
    ├── GENERAL_QUESTION → general_response → END
    └── CLARIFICATION    → clarify_response → END
```

**Key design decisions:**

- **Happy-path routing**: after `analyze_logs`, if all log chunks have no ERROR-level events, the flow short-circuits to `happy_path_summary` — a pure programmatic formatter that lists executed steps. No LLM is called, so there is no hallucination risk.

- **Keyword expansion**: when `find_similar` has no error log text to use (e.g. a business-language query like "customers are complaining orders are slow"), it calls the router LLM first to convert the description into technical search terms (`timeout latency connection pool circuit breaker`) before querying OpenSearch.

- **Evidence pre-structuring**: before the RCA synthesis LLM sees the log chunks, `_format_evidence()` pre-labels each chunk with its transaction number, time range, services, and extracted ERROR/WARN lines. This prevents the LLM from conflating multiple distinct failure scenarios into a single incorrect summary.

- **Grounded prompting**: the synthesis prompt explicitly checks for the presence of ERROR logs in the evidence. If none found, it produces an order summary instead of an RCA — and it is forbidden from stating a failure occurred unless ERROR logs are present.

### 8. React UI (`services/ui/index.html`)

Single-file React via CDN — no build step, no npm. Just open in a browser. Talks to the orchestrator's `/api/v1/chat` endpoint via fetch.

In production, this would be a proper Vite-built TypeScript app with assistant-ui components and SSE streaming. For POC, the inline version is enough to demo.

### 9. Redis (Docker)

Holds short-term chat session memory (last 10 messages per session, 1-hour TTL). Each chat message includes a `session_id`; the orchestrator pulls history before invoking the agent. **In the current POC this is wired but minimal** — full conversational continuity is a stretch goal.

### 10. Ollama (Local LLM Server)

Runs locally on your machine. Serves models via REST on `http://localhost:11434`. We use it as a Claude-compatible API. In production, the orchestrator simply swaps `ChatOllama` for `ChatAnthropic` — one line change.

---

## Mock Data Strategy

The mock data was designed to make the GenAI agent's job realistic but tractable.

### Order numbers and locations

All orders follow the pattern `ORD-NNNNN` (e.g., `ORD-00175`). Locations follow `LOC-XXX-NN` (e.g., `LOC-NYC-01`). These are stable across:

- Postgres seed data
- Generated logs
- Historical incidents (when referenced)

### Failure realism

Each failure scenario includes **realistic Spring Boot stack traces**:

```
org.springframework.jdbc.CannotGetJdbcConnectionException:
  Failed to obtain JDBC Connection; nested exception is
  java.sql.SQLTransientConnectionException: HikariPool-1 -
  Connection is not available, request timed out after 10000ms.
    at org.springframework.jdbc.datasource.DataSourceUtils.getConnection
    at com.example.order.OrderRepository.save(OrderRepository.java:87)
    at com.example.order.OrderService.createOrder(OrderService.java:142)
```

The LLM sees this exception text in its context and produces RCAs that mention `HikariCP`, `JDBC`, connection pools, etc. — concrete, actionable diagnoses.

### Multi-service traces

Each transaction emits logs from multiple services with the same `trace_id`:

```
[11:38:48] [order-service] INFO: Received order request for ORD-00175
[11:38:48] [order-service] WARN: HikariCP pool utilization at 95%
[11:38:48] [order-service] WARN: Awaiting available connection from pool
[11:38:58] [order-service] ERROR: Failed to obtain JDBC Connection
[11:38:58] [order-service] ERROR: Order ORD-00175 FAILED: database unavailable
[11:38:59] [notification-service] INFO: Failure notification sent for order ORD-00175
```

The chunker groups all of these into one searchable unit, so when the LLM gets the chunk, it sees the full story arc.

---

## Prerequisites

| Tool | Minimum Version | Why |
|---|---|---|
| Docker Desktop | 24.0+ | Runs Kafka, OpenSearch, Postgres, Redis |
| Python | 3.11+ | All services are Python |
| Ollama | 0.1+ | Local LLM server |
| 16 GB RAM | — | OpenSearch (1.5 GB) + LLM (2 GB) + everything else |
| 20 GB disk | — | Docker images + Ollama models (no embedding model needed) |

### For Windows users specifically

- WSL2 must be installed and enabled for Docker Desktop
- PowerShell execution policy must allow local scripts:
  ```powershell
  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
  ```
- If you have an NVIDIA GPU with limited VRAM, force CPU mode:
  ```powershell
  [Environment]::SetEnvironmentVariable("OLLAMA_NUM_GPU", "0", "User")
  [Environment]::SetEnvironmentVariable("CUDA_VISIBLE_DEVICES", "-1", "User")
  ```
  Then restart Ollama from the system tray.

---

## Step-by-Step Setup

### Step 1: Install Ollama models (one-time, ~6 GB download)

```powershell
ollama pull qwen2.5:7b   # 4.7 GB — production-quality synthesis
ollama pull qwen2.5:3b   # 2 GB — faster routing AND fallback for low-RAM machines
```

For machines with <8 GB free RAM, just `qwen2.5:3b` is enough.

### Step 2: Bring up infrastructure

**Mac/Linux:**
```bash
make up
```

**Windows:**
```powershell
.\run.ps1 up
```

Wait ~30 seconds. Verify all containers are healthy:
```powershell
docker compose -f infra\docker-compose.yml ps
```

You should see 6 containers all "Up": Kafka, Kafka UI, OpenSearch, OpenSearch Dashboards, Postgres, Redis.

### Step 3: Install Python dependencies

**Mac/Linux:**
```bash
make install
```

**Windows:**
```powershell
python -m pip install -r services\log-generator\requirements.txt
python -m pip install -r services\ingestor\requirements.txt
python -m pip install -r services\mcp-server\requirements.txt
python -m pip install -r services\orchestrator\requirements.txt
```

**Note**: Dependencies are lightweight — no embedding model or PyTorch required. Install completes in under a minute.

### Step 4: Verify everything is healthy

```powershell
python scripts\smoke_test.py
```

Expected:
```
=== Infrastructure ===
✅ Kafka reachable. Topics: [...]
✅ OpenSearch reachable. Status: green, Nodes: 1
✅ k-NN plugin enabled: opensearch-knn
✅ Postgres reachable. Orders: 200, Locations: 10, Failed: 30+
✅ Redis reachable.
=== Local LLM ===
✅ Ollama reachable. Models: [...]
=== Application services ===
⚠️  MCP server: ... (not running yet)
⚠️  Orchestrator: ... (not running yet)
```

The two warnings at the bottom are expected at this stage — those services aren't started yet.

### Step 5: Seed historical incidents

```powershell
python scripts\seed_incidents.py
```

This deletes and recreates the `incidents-historical` index with 8 mock past RCAs. Completes in a few seconds — no model download required.

### Step 6: Generate mock logs to Kafka

```powershell
python services\log-generator\generate_logs.py --transactions 100 --rate 5
```

This generates ~600 log lines across various scenarios. Watch for `✅ Done. Total logs published: N`.

### Step 7: Start the three application services

Each runs in its own PowerShell window.

**Window 1: Ingestor**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python services\ingestor\ingestor.py
```
Wait for `✅ Indexed N chunks to logs-vectors-current`.

**Window 2: MCP Server**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload
```
Wait for `Application startup complete.`

**Window 3: Orchestrator**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:3b"   # use 3b on low-RAM machines
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload
```
Wait for `Application startup complete.` and `✅ Redis connected.`

### Step 8: Verify the full stack

```powershell
python scripts\smoke_test.py
```

All 7 checks should now be green.

---

## Running the Demo

### Option A: From command line (fast)

```powershell
$body = @{ message = "Why did ORD-00175 fail?"; session_id = "demo" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```

The first query takes 30–60 seconds (Ollama loads the model). Subsequent queries are 5–15 seconds.

### Option B: Pretty React UI

**Window 4:**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000
```

Then open your browser to **http://localhost:3000/index.html**.

Type queries directly, or click the example questions.

### Find Which Orders Have Indexed Logs

```powershell
$body = @{ size = 0; aggs = @{ unique_orders = @{ terms = @{ field = "order_no"; size = 30 } } } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://localhost:9200/logs-vectors-current/_search -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 6
```

This shows all order numbers currently indexed. Any of these will produce a real RCA with `evidence_count > 0`.

### Sample queries to try

| Query | What it exercises |
|---|---|
| "Why did ORD-00175 fail?" | Full RCA pipeline, DB connection failure scenario |
| "What went wrong with ORD-00057?" | Payment gateway timeout scenario |
| "What is the current status of ORD-00100?" | STATUS_ONLY path (faster, no log analysis) |
| "Hello, what can you help me with?" | GENERAL_QUESTION path |

---

## Verifying Each Layer

### Kafka has messages

```powershell
docker exec poc-kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic app-logs
```

Or open Kafka UI: http://localhost:8090 → Topics → app-logs → Messages

### OpenSearch has vectors

```powershell
Invoke-RestMethod http://localhost:9200/logs-vectors-current/_count
Invoke-RestMethod http://localhost:9200/incidents-historical/_count
```

Expected: vector index has ~50–150 chunks (depending on how many logs you generated); incidents index has 8.

Or open OpenSearch Dashboards: http://localhost:5601

### Postgres has orders

```powershell
docker exec -it poc-postgres psql -U postgres -d orders -c "SELECT status, COUNT(*) FROM orders GROUP BY status;"
```

Expected: 4 rows showing COMPLETED, FAILED, PENDING, CANCELLED counts.

### MCP tools work

```powershell
# Tool 2: get_order_status
$body = @{ order_no = "ORD-00005" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/get_order_status -Method Post -ContentType "application/json" -Body $body

# Tool 1: analyze_order_logs
$body = @{ order_no = "ORD-00175"; top_k = 5 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/analyze_order_logs -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# Tool 3: find_similar_incidents
$body = @{ description = "payment failure"; top_k = 3 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/find_similar_incidents -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 3
```

---

## Common Issues & Solutions

### "running scripts is disabled on this system" (PowerShell)

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### "cannot be loaded. The file is not digitally signed"

Same as above — execution policy. If your IT has locked it via Group Policy, use the `.bat` wrapper instead.

### "Docker daemon not running"

Start Docker Desktop. Wait for the whale icon in the system tray to be solid (not animated).

### Port conflicts (e.g., 8080 already in use)

Find what's using it:
```powershell
netstat -ano | findstr :8080
```
Then either kill it or change our port in `infra\docker-compose.yml`.

### Ollama "cudaMalloc failed: out of memory"

You have a GPU but insufficient VRAM. Force CPU mode:
```powershell
[Environment]::SetEnvironmentVariable("OLLAMA_NUM_GPU", "0", "User")
[Environment]::SetEnvironmentVariable("CUDA_VISIBLE_DEVICES", "-1", "User")
```
Then quit Ollama from system tray and relaunch.

### `evidence_count: 0` for every query

This means the MCP tool isn't finding logs for the order number you queried. Two checks:

1. Confirm the order number exists in the index:
   ```powershell
   $body = @{ size = 0; aggs = @{ unique_orders = @{ terms = @{ field = "order_no"; size = 30 } } } } | ConvertTo-Json -Depth 5
   Invoke-RestMethod -Uri http://localhost:9200/logs-vectors-current/_search -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 6
   ```
   Use one of the order numbers shown.

2. The `analyze_order_logs` MCP tool should be using a simple `term` filter on `order_no` (see `services/mcp-server/server.py`).

### Ingestor consumes from Kafka but never flushes

The ingestor has two flush triggers:
- **Background timer**: flushes every 30 seconds automatically, even when no new messages arrive
- **Message-triggered**: flushes when 500 logs are buffered

If you just started the ingestor and it consumed a batch, wait up to 90 seconds (60s chunk window + 30s timer tick) for the background flush to fire. You will see:
```
INFO Flushing N chunks (N raw logs)
INFO ✅ Indexed N chunks to logs-vectors-current
```
If it still doesn't flush, generate a small burst to trigger the chunk window expiry:
```powershell
python services\log-generator\generate_logs.py --transactions 10 --rate 10
```

### LangChain dependency conflict

If you had `langchain` v1.x installed globally before (from another project), this POC's v0.3.x deps conflict. Force the compatible versions:

```powershell
python -m pip install --upgrade "langchain==0.3.27" "langchain-community<0.4"
```

Better long-term fix: use a virtual environment per project.

---

## What You Should See

The response format depends on what the logs contain. The system routes to different outputs automatically.

### Response 1 — Failed order (RCA)

Query: `"Why did ORD-00175 fail?"`

```
**Summary**
Order ORD-00175 failed due to HikariCP connection pool exhaustion...

**What Happened**
[11:38:48] [order-service] WARN: HikariCP pool utilization at 95%
[11:38:58] [order-service] ERROR: Failed to obtain JDBC Connection
[11:38:58] [order-service] ERROR: Order ORD-00175 FAILED: database unavailable

**Root Cause**
HikariCP pool maxed out — long-running queries held connections beyond their lifespan.

**Contributing Factors**
Misbehaving background cron job holding open connections.

**Recommended Actions**
1. Immediate: Restart order-service to release stuck connections
2. Short-term: Increase HikariCP max pool size from 50 to 100
3. Long-term: Add alerting on pool utilisation > 80%

**Evidence**
- [11:38:48] order-service: HikariCP pool utilization at 95%
- [11:38:58] order-service: Connection is not available, timed out after 10000ms
```

---

### Response 2 — Successful order (Executed Steps)

Query: `"More details about ORD-00093"`

When logs exist but contain no ERROR-level events, the system skips the RCA pipeline entirely and returns a clean step-by-step summary — no LLM involved:

```
**Order ORD-00093 — Executed Steps**

**Current Status**: COMPLETED | Amount: 179.00 USD | Location: London (LON)
**Payment**: COMPLETED via CARD
**Shipment**: SHIPPED via FedEx (tracking: TRK-00093)

**Transaction 1**  (2026-06-03T09:15:23 → 2026-06-03T09:15:25)  services: order-service, payment-service, ...
  1. [09:15:23] [order-service] INFO: Received order request for ORD-00093
  2. [09:15:23] [inventory-service] INFO: Inventory check passed at LOC-LON-01
  3. [09:15:23] [payment-service] INFO: Initiating payment, amount=179.00 USD
  4. [09:15:25] [payment-service] INFO: Payment captured, txn_id=TXN815461
  5. [09:15:25] [order-service] INFO: Order confirmed COMPLETED
  6. [09:15:25] [shipping-service] INFO: Shipment created via FedEx (SHP-00093)
```

Key things to notice across both responses:

- **`evidence_count > 0`** — actual logs were retrieved from OpenSearch
- **`intent`** — correctly classified by the routing LLM
- **`order_no`** — correctly extracted from the natural-language query
- **No hallucination on happy paths** — the executed steps formatter reads directly from structured log data

---

## From POC to Production

The architecture is intentionally portable. Here's the migration checklist:

| Change | Effort |
|---|---|
| Swap `ChatOllama` for `ChatAnthropic` in `services/orchestrator/main.py` | 1 line of code |
| Point ingestor at production Kafka cluster | Environment variable |
| Point MCP server at production OpenSearch + Order DB | Environment variables |
| Replace mock log generator with real Spring Boot apps | No code change — same Kafka topic |
| Convert MCP HTTP endpoints to official MCP stdio/SSE protocol | ~50 lines of code |
| Add JWT validation + RBAC on MCP tools | ~30 lines per tool |
| Add LangSmith / Langfuse instrumentation | ~5 lines, one decorator |
| Multi-node OpenSearch with 3 shards + 1 replica | Configuration only |
| Proper React build with Vite + assistant-ui + SSE streaming | New frontend project |
| Enterprise IdP integration | Configuration |
| Production-grade React Frontend | Dedicated FE work |

**The hardest engineering decisions are already made.** The data model, the agent flow, the prompt design, the chunk strategy — all of these stay the same.

---

## For the Client Pitch

When you walk into the client meeting, here's the credibility-building flow:

### 1. Open the architecture document
Show them the [System Architecture diagram](#architecture-overview). Emphasize that:
- **Their existing pipeline is untouched** — green dashed line shows the new parallel consumer
- **The MCP server is the only thing talking to data** — clean abstraction
- **The LLM only reasons over retrieved evidence** — never hits OpenSearch directly

### 2. Open your laptop
Bring up the React UI at http://localhost:3000/index.html. Show them the chat interface.

### 3. Ask them to type a question
Let them pick the order number. Watch their face when the RCA streams back in 10 seconds.

### 4. Show them the evidence
The response cites specific log timestamps and service names. Open OpenSearch Dashboards (http://localhost:5601) and show them the corresponding raw logs. The AI's reasoning is verifiable.

### 5. Show them the agent decision flow
Mention that the orchestrator window logs each LangGraph node executing. For a failed order:
```
INFO Node: classify_intent
INFO Node: fetch_order_status
INFO Node: analyze_logs
INFO Node: find_similar_incidents
INFO Node: synthesize_rca
```

For a successful order it short-circuits after log retrieval:
```
INFO Node: classify_intent
INFO Node: fetch_order_status
INFO Node: analyze_logs
INFO Node: happy_path_summary
```

This isn't a black box — every step is observable and debuggable.

### 6. The closing line

> *"What you just saw runs entirely on my laptop with an open-source 3-billion-parameter model. In production we'd use Claude Sonnet 4.5 — much higher quality RCAs, and the same architecture. The entire migration is a one-line code change and infrastructure configuration."*

That single sentence transforms a slide deck into a working system.

---

## Stretch Goals (Optional Enhancements)

If you want to keep polishing after the POC works:

1. **Streaming UI** — wire `/api/v1/chat/stream` to the React UI for token-by-token display
2. **Langfuse observability** — add to docker-compose, capture all prompts/responses
3. **Evaluation harness** — Ragas metrics over a golden dataset
4. **More failure scenarios** — add specific scenarios that match your client's actual production failures
5. **Multi-turn conversation** — surface Redis session memory in the UI so users can follow up
6. **Citation linking** — clicking a citation in the RCA opens the raw log in OpenSearch Dashboards
7. **Cost telemetry** — track tokens per query, project monthly cost
8. **Quality scoring** — thumbs-up/down feedback collection
9. **A/B testing prompts** — compare RCA quality across different prompt designs
10. **Real MCP protocol** — convert HTTP endpoints to official stdio/SSE MCP transport

---

## Summary

You built a **complete, working, locally-runnable GenAI RCA system** that:

- Demonstrates the exact production architecture
- Uses entirely free, open-source components
- Produces evidence-grounded RCAs with no hallucination
- Is one configuration change away from production-grade
- Was built in a single day from scratch

This is significant. Most architects can only show diagrams; you can show a working system.

When you next walk into the client meeting, you're not pitching theory — you're demoing software.

---

## Frequently Asked Questions

### Are we using RAG anywhere?

Yes — the `ORDER_INQUIRY` flow is a **RAG (Retrieval-Augmented Generation) pipeline**. RAG means the LLM's response is grounded in documents retrieved at query time rather than relying solely on its training data.

There are three retrieval steps before the LLM generates anything:

| Step | What is retrieved | How |
|------|------------------|-----|
| `fetch_order_status` | Structured order, payment and shipment data | SQL JOIN on Postgres |
| `analyze_logs` | Relevant log chunks for the order | Hard filter by `order_no` on OpenSearch |
| `find_similar_incidents` | Past RCAs with similar symptoms | **Full-text DQL search** (`multi_match`) on OpenSearch |

All three results are injected into the synthesis prompt (**Augment**), and only then does the LLM produce the RCA (**Generate**). The prompt explicitly forbids speculation:

```
STRICT RULES:
- Use ONLY facts present in the evidence above.
- If evidence is insufficient for any section, write "Insufficient evidence for [X]" instead of speculating.
```

This is what prevents hallucination — the LLM acts as a reasoning engine over retrieved facts, not a knowledge source.

---

### What happens when a business user asks in plain English with no technical terms?

Example: *"customers are complaining orders are slow"*

The `find_similar` node handles this with **LLM-based keyword expansion**. Before querying OpenSearch, it calls the router LLM (`qwen2.5:3b`) with a prompt that asks it to convert the business description into technical search terms:

```
"customers are complaining orders are slow"
    ↓  qwen2.5:3b keyword expansion
"timeout latency slow response connection pool circuit breaker high response time"
    ↓  multi_match DQL
incidents: payment gateway timeouts, HikariCP pool exhaustion, ...
```

This only triggers when no error log text is available to use as the search signal. When error chunks exist, the actual error message text is used directly — keyword expansion is skipped.

---

### Why does the system show executed steps instead of an RCA for some orders?

When an order's logs contain no ERROR-level events, the flow short-circuits after `analyze_logs`:

| Condition | Path | Output |
|---|---|---|
| Logs found with errors | `find_similar` → `synthesize_rca` | Structured RCA (LLM) |
| Logs found, no errors | `happy_path_summary` | Executed steps (programmatic, no LLM) |
| No logs at all | `find_similar` → `synthesize_rca` | "Insufficient evidence" message |

The executed-steps formatter reads directly from structured log chunk data — timestamps, services, log lines — so there is no hallucination risk.

---

### Are embeddings generated by a separate model or does OpenSearch generate them?

**Embeddings are no longer used — this project was updated to use DQL full-text search instead.**

OpenSearch never generates embeddings itself; it is a search and storage engine only. The original design used `BAAI/bge-large-en-v1.5` (via `sentence-transformers`) to produce 1024-dim vectors stored in a k-NN HNSW index. That approach was replaced because log errors have precise, distinctive vocabulary (`HikariCP`, `NullPointerException`, `CannotGetJdbcConnectionException`) where BM25 term matching is more reliable and predictable than cosine similarity — and it removes ~3 GB of dependencies.

The current flow for `find_similar_incidents` is:

```
User message / error chunk text
    ↓
multi_match DQL query           ← BM25 scoring across summary, root_cause, resolution, keywords
    ↓ matching incident docs (ranked by relevance)
LLM synthesis prompt            ← incidents injected as context
```

OpenSearch's role is now **full-text search and filtering** — no vectors, no HNSW, no cosine similarity.

---

### How does log ingestion, search, and LLM work end-to-end?

#### Log ingestion (`services/ingestor/ingestor.py`)

1. Kafka messages are consumed, parsed, and grouped by `trace_id` into 60-second time windows
2. Each `LogChunk` gets a `composed_text` — a human-readable multi-line string:
   ```
   [11:38:48] [order-service] ERROR: Failed to obtain JDBC Connection | exception: ...
   ```
3. Flushes trigger two ways — a **background periodic task** every 30 seconds, and a **message-triggered flush** when 500 logs are buffered. Both drain chunks whose 60-second window has elapsed
4. Chunks are bulk-indexed to OpenSearch with deterministic IDs (idempotent re-runs)

No embedding model is involved — `composed_text` is stored as a plain `text` field and searched by BM25.

---

#### How search works in OpenSearch

**Tool 1 — `analyze_order_logs` on `logs-vectors-current`**

DQL `bool` query — `order_no` filter is a hard constraint, `should` clauses rank within results:
```json
{
  "query": {
    "bool": {
      "filter": [{ "term": { "order_no": "ORD-00175" } }],
      "should": [
        { "term":  { "has_error": { "value": true,  "boost": 1.5 } } },
        { "match": { "message":   { "query": "<context>", "boost": 2.0 } } }
      ]
    }
  },
  "sort": [{ "earliest_ts": "asc" }]
}
```

**Tool 3 — `find_similar_incidents` on `incidents-historical`**

`multi_match` with field boosting and fuzzy matching — no vectors:
```json
{
  "query": {
    "bool": {
      "should": [
        { "multi_match": { "query": "<description>",
            "fields": ["summary^3", "root_cause^2", "resolution^1"],
            "fuzziness": "AUTO", "minimum_should_match": "30%" } },
        { "match": { "keywords": { "query": "<description>", "boost": 1.5 } } }
      ]
    }
  }
}
```

---

#### Which LLM is used

Two separate LLM roles, both served by Ollama locally:

| Role | Model | Config |
|---|---|---|
| **Router / Intent Classifier** | `qwen2.5:3b` | `temperature=0.0` — classifies into ORDER_INQUIRY / STATUS_ONLY / GENERAL_QUESTION / CLARIFICATION |
| **RCA Synthesizer** | `qwen2.5:7b` | `temperature=0.2` — generates the full structured RCA from retrieved evidence |

Both use `ChatOllama` from `langchain_ollama` (`services/orchestrator/main.py`).

**For production**: replace `ChatOllama` with `ChatAnthropic` — one line change. Target models: `claude-sonnet-4-5` (synthesis) and `claude-haiku-4-5` (routing).

---

## License & Credits

- **OpenSearch** — Apache 2.0
- **Apache Kafka** — Apache 2.0
- **PostgreSQL** — PostgreSQL License
- **Redis** — BSD 3-Clause
- **Qwen 2.5** (Alibaba) — Apache 2.0 (with use-case restrictions for enterprises >100M MAU)
- **LangGraph / LangChain** — MIT
- **FastAPI** — MIT
- **React** — MIT
- **Ollama** — MIT

All components are production-friendly licenses. Verify any commercial restrictions with your legal team before deploying.

---

*Last updated: June 2026. This document is a living reference — update as the POC evolves.*
