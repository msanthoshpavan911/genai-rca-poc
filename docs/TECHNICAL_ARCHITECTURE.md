# Technical Architecture Specification: GenAI Log Analysis & Root Cause Analysis (RCA) Assistant

---

## Executive Summary

The **GenAI Log Analysis & Root Cause Analysis (RCA) Assistant** is an enterprise-grade automated diagnostic system built on top of **existing, read-only production OpenSearch log indices**. Rather than introducing a custom log ingestion or vector indexing pipeline, the system interfaces directly with live client logs, using a two-step `loggingId` correlation strategy to extract full transaction traces.

The solution operates through two primary modes:
1. **Interactive Chat Diagnostic Engine**: An on-demand, persona-aware assistant that answers user questions about specific orders or transaction errors with grounded, evidence-backed RCAs.
2. **Proactive Error Detection & Alerting Engine**: A background worker that continuously scans client log streams, deduplicates error traces, synthesizes root cause analyses, and dispatches actionable alerts to engineering distribution lists (Email) and Slack channels.

---

## Architectural Principles & Key Design Decisions

```
+-----------------------------------------------------------------------------------+
|                            CORE DESIGN PRINCIPLES                                 |
+-----------------------------------------------------------------------------------+
| 1. Read-Only Target Ingestion      | Zero footprint on existing client OpenSearch.   |
| 2. Guaranteed Zero-Hallucination   | Deterministic short-circuit for happy paths.    |
| 3. Two-Step Correlation Retrieval  | Anchor text search -> loggingId trace expansion.|
| 4. BM25 Full-Text RAG (No Vectors) | Precise term matching for log exceptions/IDs.   |
| 5. Persona-Aware Prompt Framing    | Dynamic phrasing for SREs vs Business users.    |
| 6. Dual-LLM Model Distribution     | Lightweight router model + Heavy synthesis model.|
+-----------------------------------------------------------------------------------+
```

1. **Read-Only Ingestion Protocol**: The system never writes to or modifies client log indices. Logs reach OpenSearch via existing client pipelines (e.g., Fluentbit). The GenAI layer only owns a single isolated index (`incidents-historical`) for incident memory.
2. **Deterministic Code Routing Over LLM Speculation**: LLM calls are strictly avoided when evidence is absent or clean. Happy paths (zero error logs) trigger deterministic code-generated summaries.
3. **BM25 Term Matching Over Vector Embeddings**: Microservice log traces contain exact exception names, HTTP status codes, and entity IDs (`ORD-XXXXX`, `loggingId`). Full-text BM25 index matching provides higher accuracy and lower system complexity than vector embeddings.
4. **Decoupled Tool Architecture (MCP Paradigm)**: All data retrieval and storage operations are abstracted behind Model Context Protocol (MCP) tool interfaces, allowing independent evolution of log stores and agent logic.

---

## High-Level System Architecture

```
                                  +---------------------------------------+
                                  |    EXISTING CLIENT INFRASTRUCTURE     |
                                  | Spring Boot Apps ---> (Fluentbit)    |
                                  |                   |                   |
                                  |                   v                   |
                                  |       Client OpenSearch Indices       |
                                  +-------------------+-------------------+
                                                      |
                                                      | READ-ONLY (msg, loggingId)
                                                      v
+-----------------------+                 +-----------+-----------+
| PRESENTATION LAYER    |                 |   MCP TOOL SERVER     |
| React Chat UI (:3000) |                 |    FastAPI (:8001)    |
| • Project Picker      +---------------->| • 6 Tool Endpoints    |<======+
| • Persona Selector    | POST /chat      | • Config Mappings     |       |
+-----------------------+ (SSE Streaming) +-----------+-----------+       |
                                                      ^                   |
                                                      | HTTP              |
                                                      v                   |
                                  +-------------------+-------------------+
                                  | GENAI ORCHESTRATION ENGINE (8000)     |
                                  | FastAPI + LangGraph State Machine     |
                                  | • Dual LLM (qwen2.5:3b & qwen2.5:7b) |
                                  +-------------------+-------------------+
                                                      ^
                                                      | POST /internal/analyze_incident
                                                      |
                                  +-------------------+-------------------+
                                  | PROACTIVE MONITORING ENGINE (Mod 3)   |
                                  | • Background Poller (every 10 min)    |
                                  | • Redis Checkpoints & Alert Dedup     |
                                  +-------------------+-------------------+
                                                      |
                                           +----------+----------+
                                           |                     |
                                           v                     v
                                    [Email SMTP DL]       [Slack Webhook]
```

---

## Component Deep Dives

### 1. User Presentation Layer (Module 4 & Module 5)
* **File**: `services/ui/index.html`
* **Tech Stack**: Single-File React, Tailwind CSS / Vanilla CSS, Server-Sent Events (SSE).
* **Key Responsibilities**:
  * **Session Guarding & Project Scoping**: Forces the user to pick a target client project before enabling chat interactions. Locks the `project_id` for the session context.
  * **Persona Selection**: Configures the persona context (`technical` vs `business`) passed with every payload to adjust the output narrative density without altering the required RCA schema.

---

### 2. GenAI Orchestration Engine (FastAPI + LangGraph)
* **File**: `services/orchestrator/main.py`
* **Port**: `8000`
* **Tech Stack**: FastAPI, LangGraph (`StateGraph`), LangChain, Ollama API (`ChatOllama`) / Anthropic API (`ChatAnthropic`).
* **State Definition (`AgentState`)**:
  ```python
  class AgentState(TypedDict):
      user_message: str
      session_id: str
      project_id: str
      persona: str
      order_no: Optional[str]
      intent: Optional[str]
      time_window_start: Optional[str]
      time_window_end: Optional[str]
      log_evidence: Optional[Dict]
      similar_incidents: Optional[List[Dict]]
      response: Optional[str]
      rca_sections: Optional[Dict[str, str]]
  ```

#### LangGraph State Machine Execution Topology

```
                   [START]
                      |
                      v
             [classify_intent]
                      |
            +---------+---------+
            |                   |
  (GENERAL / CLARIF)     (ORDER_INQUIRY)
            |                   |
            v                   v
     [General Response]   [analyze_logs] (via MCP)
            |                   |
            +----+--------------+---------------+
                 |                              |
           (No Evidence)                  (Clean Logs)
                 |                              |
                 v                              v
           [no_evidence]               [happy_path_summary]
                 |                              |
                 v                              v
               [END]                          [END]
                                                ^
                                                |
                                         (Errors Found)
                                                |
                                                v
                                         [find_similar] (via MCP)
                                                |
                                                v
                                         [synthesize_rca]
                                                |
                                                v
                                         [save_incident] (via MCP)
```

#### Node Operations Matrix

| Node Name | Tool Calls / Invocation | Description |
|---|---|---|
| `classify_intent` | `ORDER_NO_PATTERN` regex + Router LLM | Extracts order ID (`ORD-XXXXX`), parses natural language date ranges into ISO start/end timestamps, classifies intent (`ORDER_INQUIRY`, `GENERAL_QUESTION`, `CLARIFICATION`). |
| `analyze_logs` | MCP `analyze_order_logs` | Executes two-step OpenSearch correlation search. |
| `happy_path_summary` | None (Deterministic Code) | Executes when logs exist with 0 errors. Formats execution trace without LLM. |
| `no_evidence` | None (Deterministic Code) | Executes when 0 log chunks are returned. |
| `find_similar` | Router LLM + MCP `find_similar_incidents` | Extracts technical keywords or error logs and runs BM25 similarity match against `incidents-historical`. |
| `synthesize_rca` | Synthesis LLM | Formats evidence, enforces `FORMAT A` directive, generates grounded RCA narrative. |
| `save_incident` | MCP `save_incident` | Regex-parses markdown sections (`Summary`, `Root Cause`, `Recommended Actions`) and writes back to `incidents-historical`. |

---

### 3. Dual-LLM Model Distribution & Prompt Strategy

To optimize latency, cost, and output quality, tasks are partitioned between two specialized LLMs:

```
+-----------------------------------------------------------------------------------+
|                               DUAL-LLM SEPARATION                                 |
+-----------------------------------------------------------------------------------+
| ROUTER MODEL (qwen2.5:3b / Claude Haiku)                                         |
| • Fast inference (<500ms)                                                         |
| • Tasks: Intent classification, ISO date/time window parsing, keyword expansion  |
+-----------------------------------------------------------------------------------+
| SYNTHESIS MODEL (qwen2.5:7b / Claude Sonnet)                                      |
| • High reasoning fidelity & strict instruction following                          |
| • Tasks: Evidence synthesis, causal reasoning, markdown section formatting         |
+-----------------------------------------------------------------------------------+
```

#### Persona Framing Directive
* **Technical SRE (`technical`)**: Emphasizes exception class names, stack trace signatures, infrastructure metrics (HikariCP pool exhaustion, socket timeouts, HTTP 5xx codes).
* **Business User (`business`)**: Translates technical errors into plain-English business impact narrative (e.g., "Payment gateway connection timed out during checkout") while preserving strict section headers for incident index parsing.

---

### 4. MCP Tool & Data Integration Layer
* **File**: `services/mcp-server/server.py`
* **Port**: `8001`
* **Tech Stack**: FastAPI, `opensearch-py` (`AsyncOpenSearch`).
* **Configuration Registry**: `services/mcp-server/project_config.py` maps `project_id` to underlying OpenSearch index aliases and log message search fields.

```python
PROJECT_CONFIGS = {
    "app_launchpad": {
        "index_name": "fluentbit-csg-gr2v_app_launchpad-alias",
        "search_field": "msg",
    },
    "payment_gateway": {
        "index_name": "fluentbit-csg-gr2v_payment_gateway-alias",
        "search_field": "message",
    }
}
```

#### MCP Tool Suite Specification

```
+----------------------------------------------------------------------------------+
|                              MCP TOOL SPECIFICATIONS                             |
+----------------------------------------------------------------------------------+
| 1. analyze_order_logs(project_id, order_no, time_window_start, time_window_end) |
|    • Step 1: Match `msg` with order_no within timestamp range. Filter top 3      |
|              distinct `loggingId` values.                                        |
|    • Step 2: Term search `loggingId.keyword` for each distinct trace to fetch    |
|              up to 200 log lines per trace sorted ascending by `@timestamp`.     |
+----------------------------------------------------------------------------------+
| 2. get_trace_by_logging_id(project_id, logging_id)                              |
|    • Direct step-2 trace fetch for proactive poller workflows.                    |
+----------------------------------------------------------------------------------+
| 3. find_new_errors(project_id, since, max_results)                               |
|    • Queries OpenSearch for `level: ERROR` after checkpoint `since`.              |
|    • Deduplicates results by `loggingId`.                                        |
+----------------------------------------------------------------------------------+
| 4. find_similar_incidents(description, top_k)                                    |
|    • BM25 `multi_match` against `incidents-historical` (summary, root_cause).    |
+----------------------------------------------------------------------------------+
| 5. save_incident(order_no, project_id, summary, root_cause, resolution, source)  |
|    • Indexes completed RCA into `incidents-historical`.                          |
+----------------------------------------------------------------------------------+
| 6. get_order_status(order_no)                                                    |
|    • Structured fallback Lookup for order entity metadata.                        |
+----------------------------------------------------------------------------------+
```

---

### 5. Proactive Monitoring & Alerting Engine (Module 3)
* **File**: `services/monitor/poller.py`
* **Tech Stack**: Python `asyncio`, Redis (`redis.asyncio`), `aiosmtplib`, `httpx`.
* **Execution Interval**: `POLL_INTERVAL_SECONDS = 600` (10 minutes).

```
  +-------------------------------------------------------------------+
  |                  PROACTIVE POLLING EXECUTION CYCLE                |
  +-------------------------------------------------------------------+
  | 1. Fetch active project list from MCP `/projects` endpoint.       |
  | 2. Read Redis checkpoint `monitor:checkpoint:<project_id>`.       |
  | 3. Invoke MCP `find_new_errors(project_id, since=checkpoint)`.     |
  | 4. For each unique `loggingId`:                                   |
  |    a. Check Redis key `monitor:alerted:<project_id>:<logging_id>`.|
  |    b. If NOT present:                                             |
  |       i. Call Orchestrator `/api/v1/internal/analyze_incident`.  |
  |       ii. Synthesize RCA via full LangGraph state machine.        |
  |       iii. Dispatch Alert -> Email DL & Slack Webhook.             |
  |       iv. Write Redis dedup key with TTL = 24 Hours.              |
  | 5. Update Redis checkpoint `monitor:checkpoint:<project_id>`.      |
  +-------------------------------------------------------------------+
```

---

## Data Models & Schema Definitions

### 1. LogChunk Model
Returned by MCP retrieval tools (`analyze_order_logs`, `get_trace_by_logging_id`):
```json
{
  "chunk_id": "trace-abc123",
  "trace_id": "trace-abc123",
  "earliest_ts": "2026-07-07T14:02:10Z",
  "latest_ts": "2026-07-07T14:02:15Z",
  "services": ["order-service", "payment-service"],
  "log_levels": ["INFO", "ERROR"],
  "has_error": true,
  "message": "[2026-07-07T14:02:10] [order-service] INFO: Received order ORD-00143\n[2026-07-07T14:02:15] [payment-service] ERROR: Connection timeout to PSP gateway",
  "score": 4.5
}
```

### 2. Historical Incident OpenSearch Schema (`incidents-historical`)
```json
{
  "mappings": {
    "properties": {
      "incident_id": { "type": "keyword" },
      "order_no": { "type": "keyword" },
      "project_id": { "type": "keyword" },
      "summary": { "type": "text", "analyzer": "standard" },
      "root_cause": { "type": "text", "analyzer": "standard" },
      "resolution": { "type": "text", "analyzer": "standard" },
      "keywords": { "type": "keyword" },
      "source": { "type": "keyword" },
      "created_at": { "type": "date" }
    }
  }
}
```

### 3. Redis Session Memory Schema
Used by the Orchestrator for persistent multi-turn chatbot conversation state:
* **Session History Key**: `session:<session_id>:history` (Redis List)
  * Stores JSON objects: `{"role": "user"|"assistant", "content": "...", "timestamp": "ISO-8601"}`
* **Session Metadata Key**: `session:<session_id>:meta` (Redis Hash)
  * Stores fields: `project_id`, `persona`, `updated_at`
* **TTL**: 86,400 seconds (24 hours), automatically updated and refreshed on every chat turn.
* **Management Endpoints**:
  * `GET /api/v1/sessions/{session_id}` - Retrieve stored session turns and metadata.
  * `DELETE /api/v1/sessions/{session_id}` - Clear session history and metadata.


---

## Sequence Flows

### 1. Interactive User Diagnostic Sequence

```
User          React UI (3000)      Orchestrator (8000)     MCP Server (8001)   OpenSearch (9200)
 |                   |                      |                      |                  |
 | -- Ask Question ->|                      |                      |                  |
 |    (ORD-00143)    | -- POST /chat ------>|                      |                  |
 |                   |    (proj, persona)   |                      |                  |
 |                   |                      | -- classify_intent ->|                  |
 |                   |                      |    (extract time window)                |
 |                   |                      |                      |                  |
 |                   |                      | -- analyze_order_logs ----------------->|
 |                   |                      |                      | Step 1: Anchor   |
 |                   |                      |                      | Step 2: Expansion|
 |                   |                      |                      |<-----------------|
 |                   |                      |<-- Return LogChunks -|                  |
 |                   |                      |                      |                  |
 |                   |                      | -- find_similar ---->|                  |
 |                   |                      |<-- Return past RCAs -|                  |
 |                   |                      |                      |                  |
 |                   |                      | -- LLM Synthesis --->|                  |
 |                   |                      |    (FORMAT A)        |                  |
 |                   |                      |                      |                  |
 |                   |                      | -- save_incident --->|                  |
 |                   |                      |                      |-- Write RCA ---->|
 |                   |<-- SSE Stream ------|                      |                  |
 |<-- Render RCA ----|                      |                      |                  |
```

---

## Technical Verification & Operations

### Local Development Environment Verification

```powershell
# 1. Start Infrastructure Containers (OpenSearch + Redis)
.\run.ps1 up

# 2. Seed Baseline Incidents Knowledge Base
.\run.ps1 seed-incidents

# 3. Seed Synthetic Client Logs (For Testing)
.\run.ps1 generate-logs

# 4. Launch Application Services (Individual Terminals)
.\run.ps1 mcp        # Port 8001
.\run.ps1 orch       # Port 8000
.\run.ps1 monitor    # Background Poller
.\run.ps1 ui         # Port 3000

# 5. Run Verification Smoke Test
.\run.ps1 smoke
```

---

## Production Deployment Migration Checklist

| Domain | Local POC Implementation | Production Target |
|---|---|---|
| **LLM Runtime** | Local Ollama (`qwen2.5:7b` / `qwen2.5:3b`) | Managed Claude API (`anthropic.claude-3-5-sonnet-20241022` & `claude-3-haiku-20240307`) via 1-line `ChatAnthropic` swap in `main.py`. |
| **MCP Protocol** | FastAPI HTTP JSON REST Endpoints | Standard MCP Transport over SSE / Stdio. |
| **OpenSearch Cluster** | Local Docker (Security Disabled) | Managed AWS OpenSearch Service / Elastic Cloud with TLS, HTTP Basic/IAM Auth. |
| **Redis Cache** | Local Redis Container | Managed AWS ElastiCache / Redis Enterprise Cluster. |
| **Observability** | Standard Python `logging` | LangSmith / Langfuse tracing for prompt optimization and LLM performance telemetry. |
| **Security** | Open CORS, no Auth | OAuth2 / OIDC Bearer Token Authentication + Project RBAC. |
