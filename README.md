# GenAI Log Analysis & RCA Assistant

A GenAI-powered root cause analysis layer over **existing, already-populated
production OpenSearch log indices** — read-only, no ingestion pipeline of its
own. Ask a plain-English question about an order and get back a structured,
evidence-grounded RCA. The same engine also runs proactively, watching for
new errors and alerting via email/Slack before anyone has to ask.

> For the full setup and day-2 operations, see:
> - **[docs/STARTUP_V2.md](docs/STARTUP_V2.md)** — first-time setup
> - **[docs/OPERATIONS_RUNBOOK_V2.md](docs/OPERATIONS_RUNBOOK_V2.md)** — restart scenarios, poller operations, troubleshooting
> - **[docs/Architecture.docx](docs/Architecture.docx)** — architecture overview for stakeholders

---

## The Five Modules

1. **Production Log Retrieval Layer** — reads directly from existing OpenSearch indices (one per client project). Two-step correlation: text-search the entity in question, then expand via `loggingId` to pull the full connected trace. No GenAI-owned ingestion pipeline.
2. **RCA Synthesis Engine** — happy-path short-circuit (no LLM, zero hallucination risk) when no errors are found; grounded, structured RCA (LLM) when errors are present. Writes every completed RCA back to `incidents-historical`.
3. **Proactive Error Detection & Alerting** — a background service polls every project's index every 10 minutes, deduped one alert per distinct `loggingId`, and pushes RCAs to email + Slack automatically.
4. **Chat UI — Project Selection & Session Scoping** — greets the user, requires a project pick before chat is reachable, locks that choice for the session.
5. **Persona-Aware Response Tuning** — a second selector (technical consultant vs. business user) changes the RCA's wording and technical depth without changing its structure.

---

## Quick Start

```powershell
# 1. Bring up local infra (OpenSearch, Postgres, Redis — no Kafka)
.\run.ps1 up

# 2. Install Python deps for all services
.\run.ps1 install

# 3. Pull the local LLM models
.\run.ps1 ollama-pull

# 4. Seed historical incidents (one-time)
.\run.ps1 seed-incidents

# 5. (Optional) seed local test data if you don't have a real index to point at yet
.\run.ps1 generate-logs
```

Then, each in its own terminal:

```powershell
.\run.ps1 mcp        # MCP server, port 8001
.\run.ps1 orch        # Orchestrator, port 8000
.\run.ps1 monitor     # Proactive error monitor (Module 3)
.\run.ps1 ui           # Chat UI, port 3000 (optional)
```

Verify:
```powershell
.\run.ps1 smoke
.\run.ps1 demo
```

Mac/Linux: the same commands exist as `make` targets (`make up`, `make install`, `make mcp`, etc.) — see the `Makefile`.

Full details, including how to point at a real client OpenSearch index instead of local test data, are in **[docs/STARTUP_V2.md](docs/STARTUP_V2.md)**.

---

## Architecture

```
EXISTING CLIENT INFRASTRUCTURE (unchanged)
  Spring Boot Apps ──(Fluentbit)──> Project OpenSearch Indices
                                           │  READ-ONLY
  ═══════════════════════════════════════▼══════════════════════
  NEW GENAI RCA LAYER

   MCP Server (6 tools) ◄──► incidents-historical (GenAI-owned index)
          │
   Orchestrator (LangGraph) ◄──► Proactive Monitor (Module 3)
   + Ollama / Claude LLM         polls every 10 min, dedups by
   Persona-aware synthesis       loggingId, alerts via email + Slack
          │
   Chat UI (React)
   Project + Persona picker
```

Every data-access path goes through the MCP server — the LLM never queries
OpenSearch or Postgres directly, it only reasons over evidence the MCP tools
retrieved. See `docs/Architecture.docx` for the full breakdown, key design
decisions, and open items to verify against real production data.

---

## Layout

```
genai-rca-poc/
├── infra/
│   └── docker-compose.yml          # Local dev infra: OpenSearch, Postgres, Redis
├── scripts/
│   ├── init.sql                    # Postgres seed schema
│   ├── seed_incidents.py           # Seeds incidents-historical (curated baseline)
│   ├── smoke_test.py               # Verifies all live services
│   └── _gen_architecture_docx.py   # Regenerates docs/Architecture.docx
├── services/
│   ├── log-generator/
│   │   ├── generate_logs.py            # Shared scenario library (no transport of its own)
│   │   ├── generate_logs_opensearch.py # Seeds local test data directly into OpenSearch
│   │   └── requirements.txt
│   ├── mcp-server/
│   │   ├── server.py               # 6 tools as HTTP endpoints
│   │   ├── project_config.py       # Per-project index/search-field config
│   │   └── requirements.txt
│   ├── orchestrator/
│   │   ├── main.py                 # FastAPI + LangGraph + Ollama, chat + proactive entry points
│   │   └── requirements.txt
│   ├── monitor/
│   │   ├── poller.py               # Module 3 — proactive error detection & alerting
│   │   └── requirements.txt
│   └── ui/
│       └── index.html              # Single-file React chatbot (project + persona picker)
├── Makefile / run.ps1 / run.bat    # Convenience commands
└── README.md                       # This file
```

---

## How RAG Works Here

Full-text search (DQL/BM25), not vector search:

1. **Retrieve** — `analyze_order_logs` text-searches the configured field (default `msg`) for the entity, expands via `loggingId` to the full trace; `find_similar_incidents` does BM25 `multi_match` against past RCAs.
2. **Augment** — retrieved evidence is labeled (timestamps, services, error lines, partial-trace warnings) and injected into the synthesis prompt.
3. **Generate** — the LLM writes the RCA using *only* the injected evidence. The FORMAT A (Root Cause Analysis) vs. FORMAT B (insufficient evidence) choice is decided deterministically in code from the evidence's confirmed error state, not left to the LLM to re-derive.

No embedding model or vector index — log error text has distinctive enough vocabulary that BM25 term matching is more reliable here, and it avoids a significant dependency footprint.

---

## Production Migration

| Local dev | Production |
|---|---|
| Ollama (`qwen2.5:7b`/`qwen2.5:3b`) | Claude Sonnet / Haiku — 1-line `ChatOllama` → `ChatAnthropic` swap |
| Local test data (`generate_logs_opensearch.py`) | Real client OpenSearch index — configure in `project_config.py` |
| HTTP "MCP" endpoints | Official MCP stdio/SSE protocol |
| Single-file React | Proper build (Vite), auth, SSE streaming |
| No observability | LangSmith / Langfuse |

See `docs/Architecture.docx` for the full list, including open items that need verification against real production data before go-live.
