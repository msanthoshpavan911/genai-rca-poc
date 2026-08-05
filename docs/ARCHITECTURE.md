# GenAI RCA — Architecture & Technical Reference

> **Purpose of this document**: Complete onboarding reference for anyone new to this project.
> After reading this you should understand what every file does, why it exists, and be able
> to trace any user request from the browser through every layer to the final response.

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Repository Layout](#3-repository-layout)
4. [Infrastructure](#4-infrastructure)
5. [End-to-End Flow: Chat Request](#5-end-to-end-flow-chat-request)
6. [End-to-End Flow: Proactive Monitor](#6-end-to-end-flow-proactive-monitor)
7. [MCP Tool Reference](#7-mcp-tool-reference)
8. [File-by-File Reference](#8-file-by-file-reference)
9. [Key Data Structures](#9-key-data-structures)
10. [LangGraph State Machine](#10-langgraph-state-machine)
11. [Prompts & LLM Roles](#11-prompts--llm-roles)
12. [Configuration Reference](#12-configuration-reference)
13. [Design Decisions](#13-design-decisions)
14. [Open Items for Production](#14-open-items-for-production)

---

## 1. What This System Does

A GenAI-powered assistant that sits **read-only** on top of an existing, already-populated
OpenSearch log index. Users ask plain-English questions about orders:

> "Why did ORD-00143 fail on 7th July?"

The system fetches the correlated log trace, runs it through a structured RCA synthesis
engine, and returns a grounded root cause analysis citing actual log lines. The same engine
also runs proactively — a background poller watches for new errors every 10 minutes and
dispatches RCAs via email and Slack before anyone has to ask.

**What it is NOT:**
- It does not own any ingestion pipeline. Logs reach OpenSearch via the client's existing
  Fluentbit setup — the GenAI layer never writes to that index.
- It does not use vector embeddings or semantic search. Pure BM25 full-text matching.
- It does not hallucinate — the LLM is only invoked when confirmed error evidence exists,
  and it is given only the actual log lines as input.

---

## 2. High-Level Architecture

```
  EXISTING CLIENT INFRASTRUCTURE (unchanged)
  ─────────────────────────────────────────────────────────────────
   Spring Boot Apps ──(Fluentbit)──► OpenSearch Index (per project)
                                       e.g. fluentbit-csg-gr2v_app_launchpad-alias

                                               │  READ-ONLY queries
  ════════════════════════════════════════════▼════════════════════
  GENAI RCA LAYER

                    ┌───────────────────────────────┐
                    │        MCP Server  :8001        │
                    │  5 HTTP tool endpoints          │◄──► incidents-historical
                    │  (analyze_order_logs, etc.)     │     (GenAI-owned index)
                    └───────────────┬───────────────┘
                                    │ HTTP (tool calls)
             ┌──────────────────────┼──────────────────────┐
             │                      │                       │
   ┌─────────▼──────────┐           │            ┌──────────▼───────────┐
   │  Orchestrator :8000 │           │            │  Proactive Monitor    │
   │  FastAPI + LangGraph│           │            │  poller.py           │
   │  Ollama / Claude LLM│           │            │  polls every 10 min  │
   └─────────┬──────────┘           │            └──────────┬───────────┘
             │ JSON response         │                       │ email / Slack
   ┌─────────▼──────────┐           │                       ▼
   │  Chat UI  :3000     │           │            Engineering DL / Slack Channel
   │  index.html (React) │           │
   └────────────────────┘           │
                                    │ Redis :6379
                          (poller checkpoints, alert dedup)
```

**Data flows:**
| Flow | Direction |
|---|---|
| Log ingestion | Fluentbit → OpenSearch (client-owned, untouched) |
| Log retrieval | Orchestrator → MCP → OpenSearch (read-only) |
| Incident history | MCP → `incidents-historical` (GenAI-owned) |
| RCA write-back | Orchestrator → MCP → `incidents-historical` |
| Proactive trigger | Monitor → MCP (find errors) → Orchestrator (synthesize) → email/Slack |
| Chat | Browser → Orchestrator → MCP → Orchestrator → Browser |

---

## 3. Repository Layout

```
genai-rca-poc/
│
├── infra/
│   └── docker-compose.yml          # Local dev: OpenSearch, OpenSearch Dashboards, Redis
│
├── scripts/
│   ├── seed_incidents.py           # One-time: loads curated RCA examples into incidents-historical
│   ├── check_opensearch.py         # Debug utility: verify OpenSearch connectivity & index counts
│   ├── smoke_test.py               # STALE — was for the old Kafka era, don't use
│   └── _gen_architecture_docx.py   # Regenerates docs/Architecture.docx from code
│
├── services/
│   │
│   ├── mcp-server/
│   │   ├── server.py               # 5 MCP tool endpoints (FastAPI, port 8001)
│   │   ├── project_config.py       # Per-project index name + search field config
│   │   └── requirements.txt
│   │
│   ├── orchestrator/
│   │   ├── main.py                 # FastAPI + LangGraph agent (port 8000)
│   │   └── requirements.txt
│   │
│   ├── monitor/
│   │   ├── poller.py               # Proactive error detection + alerting (Module 3)
│   │   └── requirements.txt
│   │
│   ├── log-generator/
│   │   ├── generate_logs.py        # Shared scenario library (happy path + 9 failure modes)
│   │   ├── generate_logs_opensearch.py  # Seeds local test data directly into OpenSearch
│   │   └── requirements.txt
│   │
│   └── ui/
│       └── index.html              # Single-file React chatbot (project + persona picker)
│
├── docs/
│   ├── ARCHITECTURE.md             # This file
│   ├── STARTUP_V2.md               # First-time setup guide
│   ├── OPERATIONS_RUNBOOK_V2.md    # Day-2 operations, restart scenarios, troubleshooting
│   └── Architecture.docx           # Stakeholder-facing version (generated by _gen_architecture_docx.py)
│
├── run.ps1 / run.bat / Makefile    # Convenience commands (up, down, mcp, orch, etc.)
└── README.md
```

---

## 4. Infrastructure

### Docker services (`infra/docker-compose.yml`)

| Container | Image | Port | Purpose |
|---|---|---|---|
| `poc-opensearch` | opensearchproject/opensearch:2.18.0 | 9200 | Log storage + `incidents-historical` index |
| `poc-osd` | opensearchproject/opensearch-dashboards:2.18.0 | 5601 | OpenSearch Dashboards UI |
| `poc-redis` | redis:7-alpine | 6379 | Poller checkpoints + alert dedup keys |

Security is disabled (`DISABLE_SECURITY_PLUGIN=true`) for local dev only. Production uses
`http_auth`, SSL, and CA certificates.

### Ollama (local LLM runtime)

Runs natively on the host machine (not in Docker). Two models:
- `qwen2.5:7b` — synthesis (writes the actual RCA narrative)
- `qwen2.5:3b` — routing (intent classification, time-window extraction, keyword expansion)

Production swap: `ChatOllama` → `ChatAnthropic` — one-line change in `main.py`.

---

## 5. End-to-End Flow: Chat Request

This is the full technical path when a user types a question in the UI.

```
Browser
  │
  │  POST /api/v1/chat
  │  { message, session_id, project_id, persona }
  ▼
Orchestrator (main.py) — FastAPI endpoint
  │
  │  Builds AgentState dict, calls:
  │  await agent.ainvoke(state)       ← agent is a compiled LangGraph graph
  ▼
LangGraph — Pregel execution loop
  │
  │  Runs nodes in sequence based on conditional edges
  │
  ├── [Node 1] classify_intent
  │     • Regex: does the message contain ORD-XXXXX?
  │       YES → intent = ORDER_INQUIRY (no LLM call for intent)
  │             + calls extract_time_window() via router LLM
  │               → parses ISO datetime range from natural language
  │               → stores time_window_start / time_window_end in state
  │       NO  → router LLM classifies: ORDER_INQUIRY / GENERAL_QUESTION / CLARIFICATION
  │
  ├── [Routing] route_after_classify
  │     ORDER_INQUIRY  → analyze_logs node
  │     GENERAL_QUESTION → general node (immediate response, no MCP)
  │     CLARIFICATION    → clarify node (immediate response, no MCP)
  │
  ├── [Node 2] analyze_logs                        ← ORDER_INQUIRY path only
  │     • Calls MCP POST /tools/analyze_order_logs
  │       Payload: { project_id, order_no, top_k=3,
  │                  time_window_start?, time_window_end? }
  │
  │     Inside MCP — analyze_order_logs:
  │       Step 1 — Anchor search (OpenSearch):
  │         match_phrase("msg", "ORD-00143")
  │         + filter: @timestamp within the time window
  │         + boost: ERROR-level logs score higher
  │         → returns up to 20 anchor hits
  │         → extracts distinct loggingId values (max 3 = MAX_TRACES)
  │
  │       Step 2 — Trace expansion (per loggingId):
  │         term filter: loggingId.keyword = "trace-abc123"
  │         + same timestamp filter
  │         → returns ALL log docs for that trace (up to 200), sorted by @timestamp asc
  │         → _build_chunk(): concatenates into one LogChunk
  │           chunk.message = "[ts] [service] LEVEL: text\n[ts] [service] ..."
  │           chunk.has_error = any doc with level=ERROR
  │           chunk.services = distinct service names in this trace
  │
  │       → Returns: { log_chunks: [LogChunk, ...], has_errors: bool }
  │
  ├── [Routing] route_after_logs
  │     log_chunks is empty → no_evidence node (immediate message, no LLM)
  │     log_chunks present, no errors → happy_path_summary node (no LLM)
  │     log_chunks present + errors → find_similar node (RCA pipeline)
  │
  ├── [Node 3a] no_evidence                        ← no logs found for the order
  │     Returns: "No log evidence found for ORD-00143 ..."
  │     → END
  │
  ├── [Node 3b] happy_path_summary                 ← logs exist, zero errors
  │     Programmatically formats the executed steps (no LLM, no hallucination risk)
  │     → END
  │
  └── [Node 3c] find_similar                       ← logs exist + confirmed errors
        • Picks description from actual error log text (best for BM25 matching)
          or calls router LLM to expand business language → technical keywords
        • Calls MCP POST /tools/find_similar_incidents
          → BM25 multi_match against incidents-historical
          → returns up to 3 past RCAs (summary, root_cause, resolution)
        │
        ├── [Node 4] synthesize_rca
        │     _format_evidence(): structures log chunks into readable sections
        │       - TRANSACTION 1 / TRANSACTION 2 blocks
        │       - Highlights ERROR/WARN lines
        │       - Adds ⚠ PARTIAL trace warning if only 1 service present
        │     _format_directive(): hard-codes FORMAT A (errors found) in code
        │       - Never lets the LLM decide whether errors exist
        │     Builds final prompt:
        │       System: "You are an expert SRE."
        │       Human:  [persona instructions]
        │               [format directive: "write FORMAT A — errors confirmed"]
        │               [formatted log evidence]
        │               [3 similar past incidents as reference]
        │     Calls synthesis LLM (qwen2.5:7b / Claude Sonnet)
        │     state["response"] = LLM output
        │
        └── [Node 5] save_incident
              _parse_rca_sections(): regex-extracts Summary / Root Cause / Recommended Actions
              Calls MCP POST /tools/save_incident
              → writes RCA to incidents-historical (tagged source="auto-generated")
              → END

Orchestrator returns JSON:
  { response, intent, order_no, evidence_count, is_rca }
```

---

## 6. End-to-End Flow: Proactive Monitor

The monitor runs entirely in the background — no user interaction.

```
poller.py starts
  │
  │  Connects to Redis, fetches project list from MCP GET /projects
  │
  └── Every 600 seconds (POLL_INTERVAL_SECONDS):
        │
        │  For each project_id:
        │
        ├── get_checkpoint(redis, project_id)
        │     → Redis key "monitor:checkpoint:app_launchpad"
        │     → Returns last-seen ISO timestamp (or "now minus 1h" on first run)
        │
        ├── POST MCP /tools/find_new_errors
        │     { project_id, since: "<checkpoint>" }
        │     → OpenSearch: range query for level=ERROR since checkpoint
        │     → Deduped by loggingId (one incident per trace, not per log line)
        │     → Returns: { incidents: [{logging_id, sample_message}], checked_until }
        │
        │  For each new incident (not in Redis "alerted" set):
        │
        ├── POST Orchestrator /api/v1/internal/analyze_incident
        │     { project_id, logging_id }
        │     → Fetches full trace via MCP get_trace_by_logging_id
        │     → Runs through the SAME synthesis engine as chat
        │       (same _format_evidence, RCA_SYNTHESIS_PROMPT, _parse_rca_sections)
        │     → Returns { response, is_rca }
        │
        ├── mark_alerted(redis, project_id, logging_id)  TTL=24h
        │     → Prevents re-alerting on the same trace within 24 hours
        │
        ├── send_email_alert(subject, rca_text)           if SMTP configured
        └── send_slack_alert(text)                        if SLACK_WEBHOOK_URL set
        │
        └── set_checkpoint(redis, project_id, checked_until)
              → Advances the window so next poll only scans new logs
```

---

## 7. MCP Tool Reference

The MCP server exposes 5 tools as HTTP POST endpoints on port 8001.
"MCP" here means the tool pattern (each tool has a defined input/output contract),
not necessarily the official MCP protocol — these are plain FastAPI endpoints,
trivially convertible to official MCP stdio/SSE transport later.

---

### Tool 1: `analyze_order_logs`
**Endpoint:** `POST /tools/analyze_order_logs`

**Purpose:** Given an order number, find and return all correlated log traces for it.
This is the primary retrieval tool — it does the two-step anchor + loggingId expansion.

**Request:**
```json
{
  "project_id": "app_launchpad",
  "order_no": "ORD-00143",
  "additional_context": "payment timeout",
  "time_window_hours": 24,
  "top_k": 3,
  "time_window_start": "2026-07-07T00:00:00Z",
  "time_window_end": "2026-07-07T23:59:59Z"
}
```

`time_window_start` / `time_window_end` override `time_window_hours` when provided.
`top_k` caps how many traces (loggingIds) are returned — default 3.

**Response:**
```json
{
  "order_no": "ORD-00143",
  "total_chunks_found": 2,
  "services_involved": ["circuit-breaker", "order-service", "payment-service"],
  "has_errors": true,
  "log_chunks": [ { ...LogChunk... } ]
}
```

Each `LogChunk.message` is all log lines for one `loggingId` concatenated in timestamp order.

**What it does NOT do:** Analysis. It returns raw evidence. The LLM does the reasoning.

---

### Tool 2: `find_similar_incidents`
**Endpoint:** `POST /tools/find_similar_incidents`

**Purpose:** BM25 full-text search against `incidents-historical` to find past RCAs
whose error description matches the current failure. Used to give the LLM context about
known patterns.

**Request:**
```json
{
  "description": "PaymentGatewayException PSP gateway unavailable circuit breaker OPEN",
  "top_k": 3
}
```

**Response:**
```json
{
  "incidents": [
    {
      "incident_id": "...",
      "summary": "...",
      "root_cause": "...",
      "resolution": "..."
    }
  ]
}
```

The description is best when it contains actual exception names from the logs — BM25
matches on distinctive technical vocabulary much more reliably than on business language.

---

### Tool 3: `save_incident`
**Endpoint:** `POST /tools/save_incident`

**Purpose:** Writes a completed RCA back to `incidents-historical`, so future
`find_similar_incidents` calls can use it as a reference.

**Request:**
```json
{
  "order_no": "ORD-00143",
  "project_id": "app_launchpad",
  "summary": "Payment gateway circuit breaker opened ...",
  "root_cause": "PSP Stripe response latency exceeded threshold ...",
  "resolution": "1. Investigate PSP SLA ...",
  "source": "auto-generated"
}
```

`source` is one of:
- `"curated"` — hand-seeded via `scripts/seed_incidents.py`
- `"auto-generated"` — written by the chatbot after a real RCA
- `"proactive-alert"` — written by the monitor poller

---

### Tool 4: `get_trace_by_logging_id`
**Endpoint:** `POST /tools/get_trace_by_logging_id`

**Purpose:** Fetch a full correlated trace when the `loggingId` is already known.
Used by the proactive monitor (which already has the `loggingId` from `find_new_errors`
and doesn't need the anchor text-search step).

**Request:**
```json
{
  "project_id": "app_launchpad",
  "logging_id": "trace-abc123def456",
  "time_window_hours": 24
}
```

**Response:** Same `LogChunk` shape as `analyze_order_logs`.

---

### Tool 5: `find_new_errors`
**Endpoint:** `POST /tools/find_new_errors`

**Purpose:** Scan a project's index for new `level:ERROR` log lines since a given
timestamp. Deduped by `loggingId` — one incident entry per distinct trace, not per
log line. Used exclusively by the proactive monitor.

**Request:**
```json
{
  "project_id": "app_launchpad",
  "since": "2026-07-21T10:00:00+00:00"
}
```

**Response:**
```json
{
  "checked_until": "2026-07-21T10:10:00+00:00",
  "incidents": [
    {
      "logging_id": "trace-abc123",
      "sample_message": "Failed to obtain JDBC Connection for order ORD-00143"
    }
  ]
}
```

The poller saves `checked_until` as the next checkpoint in Redis.

---

## 8. File-by-File Reference

### `services/orchestrator/main.py`

The brain of the system. Three distinct responsibilities in one file:

**1. FastAPI application**
- `GET /healthz` — liveness probe
- `GET /api/v1/projects` — passthrough to MCP `/projects` (UI calls this to populate the picker)
- `POST /api/v1/chat` — non-streaming chat endpoint (returns full JSON response)
- `POST /api/v1/chat/stream` — SSE streaming version (emits node-by-node progress events)
- `POST /api/v1/internal/analyze_incident` — called by the proactive monitor, not the UI

**2. LangGraph agent (`build_graph()`)**

Builds and compiles a `StateGraph(AgentState)`. The compiled graph (`agent`) is reused
for every request. Nodes and their roles:

| Node | Function | LLM? |
|---|---|---|
| `classify` | Extract order number, detect intent, extract time window | Router LLM (only for time extraction on ORDER_INQUIRY) |
| `analyze_logs` | Call MCP `analyze_order_logs`, store evidence in state | No |
| `no_evidence` | Return "not found" message when no logs exist | No |
| `happy_path_summary` | Format clean execution steps for error-free orders | No |
| `find_similar` | Call MCP `find_similar_incidents` | Router LLM (only for keyword expansion when no error text) |
| `synthesize` | Build prompt, call synthesis LLM, produce RCA | Synthesis LLM |
| `save_incident` | Call MCP `save_incident`, parse RCA sections | No |
| `clarify` | Return "please provide order number" message | No |
| `general` | Return "I'm specialized in order analysis" message | No |

**3. Prompts**

| Constant | Used by | Purpose |
|---|---|---|
| `INTENT_CLASSIFIER_PROMPT` | `classify_intent` | ORDER_INQUIRY / GENERAL_QUESTION / CLARIFICATION |
| `TIME_WINDOW_EXTRACTION_PROMPT` | `extract_time_window` | Parse date/time hint from natural language |
| `KEYWORD_EXPANSION_PROMPT` | `find_similar` | Translate business language → technical search terms |
| `RCA_SYNTHESIS_PROMPT` | `synthesize_rca` | The full RCA generation template (FORMAT A / FORMAT B) |
| `PERSONA_INSTRUCTIONS` | `synthesize_rca` | Wording/depth modifier per audience |

**Key helper functions:**

| Function | What it does |
|---|---|
| `extract_order_no(text)` | Regex: finds `ORD-NNNNN` pattern in the user's message |
| `extract_time_window(message)` | Router LLM call → ISO datetime pair or `(None, None)` |
| `_format_evidence(chunks)` | Renders LogChunks into a structured text block for the prompt |
| `_format_directive(chunks)` | Hard-codes FORMAT A / FORMAT B choice in code (never left to LLM) |
| `_parse_rca_sections(rca_text)` | Regex-extracts Summary/Root Cause/Recommended Actions from LLM output |

---

### `services/mcp-server/server.py`

The data access layer. All OpenSearch queries live here — the orchestrator never
queries OpenSearch directly.

**Key internals:**

| Function | Purpose |
|---|---|
| `_compose_line(doc, search_field)` | Renders one raw OpenSearch document → `[ts] [service] LEVEL: text` string |
| `_fetch_trace(index, logging_id, since, until)` | Term-filter query on `loggingId.keyword`, returns all docs sorted by `@timestamp` |
| `_build_chunk(logging_id, docs, ...)` | Folds all docs for one trace into a `LogChunk` (concatenates lines, extracts metadata) |

**Configuration constants:**
- `LOGGING_ID_FIELD` = `"loggingId.keyword"` — the exact OpenSearch field for trace correlation
- `MAX_TRACES` = 3 — cap on distinct loggingIds per order (prevents token explosion)

---

### `services/mcp-server/project_config.py`

Maps project IDs to their OpenSearch index names and search fields.
Every project has:
- `display_name` — shown in the UI picker
- `index` — the OpenSearch index or alias to query (overridable via env var)
- `search_field` — which field holds the searchable log text (default `"msg"`)

To add a new project: add one dict entry here. No other files need to change.
The poller fetches the project list dynamically from `GET /projects` on every cycle.

---

### `services/monitor/poller.py`

Standalone async process. Runs a `while True` loop with `asyncio.sleep(POLL_INTERVAL_SECONDS)`.

**Redis key structure:**
```
monitor:checkpoint:<project_id>           →  ISO datetime string (last scanned until)
monitor:alerted:<project_id>:<loggingId>  →  "1"  (TTL = ALERTED_TTL_SECONDS = 24h)
session:<session_id>:history              →  List of JSON turn objects (TTL = 24h)
session:<session_id>:meta                 →  Hash of session metadata (TTL = 24h)
```

**Graceful degradation:** Notifications (email, Slack) are fully optional. If
`SMTP_HOST` or `SLACK_WEBHOOK_URL` are not set, the poller logs a warning and
skips that channel — detection and synthesis still run and are logged.

---

### `services/log-generator/generate_logs.py`

Shared library of 10 scenario functions. Not a runnable script — imported by
`generate_logs_opensearch.py`.

| Scenario | Weight | What it generates |
|---|---|---|
| `scenario_happy_path` | 50% | 7 INFO logs, ends with `status=COMPLETED` |
| `scenario_payment_gateway_timeout` | 10% | Circuit breaker opens after 2 PSP retry failures |
| `scenario_db_connection_failure` | 8% | HikariCP pool exhausted |
| `scenario_inventory_mismatch` | 8% | Stale cache, actual stock = 0 |
| `scenario_null_pointer` | 6% | NPE in payment validator |
| `scenario_fraud_check_block` | 6% | Fraud score > threshold |
| `scenario_downstream_503` | 5% | All shipping carriers return 503 |
| `scenario_race_condition` | 3% | Optimistic locking failure |
| `scenario_oom` | 2% | Java heap space OutOfMemoryError |
| `scenario_json_parse_error` | 2% | Malformed request payload |

---

### `services/log-generator/generate_logs_opensearch.py`

Seeds the local OpenSearch instance with fake but realistic logs using the production
Fluentbit field schema: `@timestamp`, `msg`, `log_message`, `level`, `logger`,
`loggingId`, `exception`, `requestURI`, etc.

Run once to populate your local index for testing:
```powershell
python services\log-generator\generate_logs_opensearch.py --transactions 100 --index fluentbit-csg-gr2v_app_launchpad-alias
```

Not used in production — logs already exist in the real index.

---

### `scripts/seed_incidents.py`

Loads 8 hand-curated RCA examples into `incidents-historical`. Run once on setup
(or to reset to the curated baseline). These are tagged `source: "curated"` so
they can be distinguished from auto-generated ones.

---

### `scripts/check_opensearch.py`

Debug utility. Connects to OpenSearch (supports auth via env vars: `OPENSEARCH_USER`,
`OPENSEARCH_PASSWORD`, `OPENSEARCH_USE_SSL`, `OPENSEARCH_CA_CERTS`) and prints:
- Cluster health
- Index list with doc counts
- Sample documents from each index

Use this to verify connectivity before starting services.

---

### `services/ui/index.html`

Single-file React app (no build step). Three screens:
1. **Greeting + project picker** — user must select a project before chatting
2. **Persona picker** — Technical consultant vs. Business user
3. **Chat window** — messages sent to `POST /api/v1/chat/stream` (SSE), rendered as markdown

The project and persona choices are locked for the session. "New session" reloads the page.

---

## 9. Key Data Structures

### `AgentState` (TypedDict — orchestrator/main.py)

The LangGraph state dict passed through every node:

```python
{
  "user_message":      str,           # original text from user
  "session_id":        str,           # browser session ID
  "project_id":        str,           # e.g. "app_launchpad"
  "persona":           str,           # "technical" or "business"
  "order_no":          str | None,    # extracted ORD-NNNNN, or None
  "intent":            str | None,    # ORDER_INQUIRY / GENERAL_QUESTION / CLARIFICATION
  "time_window_start": str | None,    # ISO datetime extracted from message, or None (= last 24h)
  "time_window_end":   str | None,    # ISO datetime upper bound, or None (= open)
  "log_evidence":      dict | None,   # AnalyzeLogsResponse as dict
  "similar_incidents": list | None,   # list of past RCA dicts
  "response":          str | None,    # final text sent to user (markdown)
  "rca_sections":      dict | None,   # {"summary", "root_cause", "resolution"} if is_rca
}
```

### `LogChunk` (Pydantic — mcp-server/server.py)

One unit of log evidence — all docs for a single `loggingId`:

```python
{
  "chunk_id":    str,   # = loggingId
  "trace_id":    str,   # = loggingId
  "earliest_ts": str,   # ISO datetime of first doc in trace
  "latest_ts":   str,   # ISO datetime of last doc in trace
  "services":    list,  # distinct service names (from "logger" field)
  "log_levels":  list,  # distinct levels seen (INFO, WARN, ERROR)
  "has_error":   bool,  # True if any doc has level=ERROR
  "message":     str,   # all log lines concatenated: "[ts] [service] LEVEL: text\n..."
  "score":       float, # anchor search relevance score
}
```

---

## 10. LangGraph State Machine

```
                    START
                      │
                  [classify]
                      │
           route_after_classify()
          ┌───────────┼───────────┐
          │           │           │
      [general]  [analyze_logs] [clarify]
          │           │           │
         END    route_after_logs() END
                ┌─────┼──────┐
                │     │      │
          [no_evidence] │  [find_similar]
                │   [happy_path_  │
               END   summary]  [synthesize]
                         │        │
                        END  [save_incident]
                                  │
                                 END
```

**Conditional routing functions:**

`route_after_classify(state)` → string:
- `ORDER_INQUIRY` → `"evidence"` → `analyze_logs`
- `GENERAL_QUESTION` → `"general"`
- `CLARIFICATION` → `"clarify"`

`route_after_logs(state)` → string:
- `log_chunks` is empty → `"no_evidence"`
- `log_chunks` present, `has_errors=False` → `"happy_path"`
- `log_chunks` present, `has_errors=True` → `"rca"` → `find_similar`

---

## 11. Prompts & LLM Roles

### Two LLMs, two roles

| LLM | Model (local) | Model (prod) | Role |
|---|---|---|---|
| Router | `qwen2.5:3b` | Claude Haiku | Fast decisions: intent, time extraction, keyword expansion |
| Synthesis | `qwen2.5:7b` | Claude Sonnet | Writes the full RCA narrative |

### Why two models?

The router makes 1–2 word decisions ("ORDER_INQUIRY", JSON with dates). A 3B model is
fast enough and cheap. The synthesis model writes several paragraphs of structured
markdown from complex evidence — it needs quality, not speed.

### What the LLM is NOT allowed to decide

Everything the code already knows from deterministic logic is removed from the LLM's
judgment:
- **Whether errors exist**: `has_error` comes from `_build_chunk()` checking `level=ERROR`
- **Which format to use** (FORMAT A vs FORMAT B): `_format_directive()` hard-codes this
- **Whether to save the incident**: `_parse_rca_sections()` checks for `**Root Cause**`
- **Intent when order number is present**: regex short-circuits before LLM is called

This removes entire classes of hallucination.

---

## 12. Configuration Reference

### Environment variables — Orchestrator

| Variable | Default | Purpose |
|---|---|---|
| `SYNTHESIS_MODEL` | `qwen2.5:7b` | Ollama model for RCA generation |
| `ROUTER_MODEL` | `qwen2.5:3b` | Ollama model for routing & extraction |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server address |
| `MCP_BASE_URL` | `http://localhost:8001` | MCP server address |
| `REDIS_URL` | `redis://localhost:6379` | Redis for session ping |
| `DEFAULT_PROJECT_ID` | `app_launchpad` | Fallback when UI doesn't send project_id |

### Environment variables — MCP Server

| Variable | Default | Purpose |
|---|---|---|
| `OPENSEARCH_HOST` | `localhost` | OpenSearch host |
| `OPENSEARCH_PORT` | `9200` | OpenSearch port |
| `INCIDENTS_INDEX` | `incidents-historical` | Where completed RCAs are stored |
| `LOGGING_ID_FIELD` | `loggingId.keyword` | Field for trace correlation (verify against real index) |
| `MAX_TRACES` | `3` | Max loggingId traces returned per order (token explosion guard) |
| `PROJECT_APP_LAUNCHPAD_INDEX` | `fluentbit-csg-gr2v_app_launchpad-alias` | Override index name |
| `PROJECT_APP_LAUNCHPAD_SEARCH_FIELD` | `msg` | Override search field |

### Environment variables — Proactive Monitor

| Variable | Default | Purpose |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | `600` | How often to scan for new errors (10 min) |
| `INITIAL_LOOKBACK_HOURS` | `1` | First-run window per project (avoids flood on cold start) |
| `ALERTED_TTL_SECONDS` | `86400` | How long a loggingId stays "already alerted" (24h) |
| `SMTP_HOST` | — | SMTP server (leave blank to skip email) |
| `ALERT_EMAIL_DL` | — | Recipient distribution list |
| `SLACK_WEBHOOK_URL` | — | Slack incoming webhook (leave blank to skip Slack) |
| `MCP_BASE_URL` | `http://localhost:8001` | MCP server address |
| `ORCHESTRATOR_BASE_URL` | `http://localhost:8000` | Orchestrator address |

---

## 13. Design Decisions

### Why read-only on the client's index?

The client's logging pipeline (Fluentbit → OpenSearch) is already running in production.
Adding a GenAI-owned ingestion step would introduce operational risk, latency, and a
dependency on the GenAI layer staying up. Reading directly means zero changes to the
existing pipeline.

### Why BM25 and not vector/semantic search?

Log error messages use precise, distinctive vocabulary — exception class names, error
codes, service names. BM25 term matching on `"PaymentGatewayException"` is more reliable
than semantic similarity here, and avoids the embedding model dependency footprint.

### Why the MCP pattern?

The orchestrator (LLM reasoning) and the MCP server (data access) are deliberately
separated. The LLM never calls OpenSearch directly — it only reasons over evidence
the MCP tools retrieved. Benefits:
- Testable: call any MCP tool directly without the LLM
- Auditable: every data access is an explicit tool call with logged inputs/outputs
- Swappable: swap OpenSearch for another store by rewriting the MCP server only

### Why loggingId-based correlation?

In a microservice system, one order (ORD-00143) can touch 4–6 services. Logs from
different services only know they're related because they share the same `loggingId`
(the request correlation field). Grouping by loggingId reconstructs the full
cross-service trace from a single plain-text anchor search.

### Time-window scoping (token explosion guard)

An order that fails repeatedly across many days would have many distinct loggingId traces.
Without scoping, every historical failure for that order would be concatenated into the
LLM prompt — a token explosion. Two guards are in place:
1. Users include a date in natural language ("on 7th July", "yesterday") → the router LLM
   converts it to an ISO datetime range → OpenSearch filters to that window
2. `MAX_TRACES=3` caps the number of traces regardless of how many match

### Why is `FORMAT A vs FORMAT B` decided in code, not by the LLM?

In early versions, the prompt told the LLM "if errors found, write FORMAT A; if not,
write FORMAT B". The LLM would weigh overall success/failure instead of the strict
"any ERROR log = FORMAT A" rule — e.g. it would produce FORMAT B for an order where
payment succeeded but shipping failed. Since `has_error` is already computed
deterministically by `_build_chunk()`, the directive is hard-coded via `_format_directive()`.

---

## 14. Open Items for Production

These are explicitly unverified assumptions about the real client index:

| Item | Why it matters | How to verify |
|---|---|---|
| `loggingId.keyword` field exists | Without the `.keyword` sub-field, term-filter expansion breaks | `GET /real-index/_mapping` |
| `msg` is the right search field | May be `log_message` in the real index | Search for a known order number in OpenSearch Dashboards |
| `loggingId` propagates across all service hops | If it doesn't, traces will be single-service and flagged as partial | Search for a known multi-service request by loggingId |
| Index pattern vs alias | Production likely uses time-based indices (`fluentbit-app-2026.07.*`) — configure an alias | Check ILM policy on the client cluster |
| SMTP + Slack credentials | Module 3 runs without them but doesn't alert | Obtain from IT/security team |
| Auth for enterprise OpenSearch | `http_auth`, TLS, CA certs | Set `OPENSEARCH_USER`, `OPENSEARCH_PASSWORD`, `OPENSEARCH_USE_SSL`, `OPENSEARCH_CA_CERTS` |
| incidents-historical retention | Index grows unbounded; same order RCA can be saved multiple times | Add ILM policy or pre-save dedup check |
