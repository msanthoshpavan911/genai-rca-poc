# Startup Guide (Current Architecture)

This replaces `STARTUP.md` for the current build. The system now reads
**already-populated production OpenSearch indices** directly — there is no
GenAI-owned Kafka ingestion pipeline or embedding model in the live flow.
Kafka and the `ingestor` service are legacy from the original POC and are
**not required** to run the system today.

---

## What Actually Runs Now

| Service | Port | Required? | Purpose |
|---|---|---|---|
| OpenSearch (Docker) | 9200 | Yes | Log indices (per project) + `incidents-historical` |
| Postgres (Docker) | 5432 | Optional | `get_order_status` tool — only if a project has a business-entity DB |
| Redis (Docker) | 6379 | Yes | Poller checkpoints/dedup (Module 3); minimal session ping in orchestrator |
| Ollama (local) | 11434 | Yes | LLM serving — `qwen2.5:7b` (synthesis), `qwen2.5:3b` (routing) |
| `services/mcp-server` | 8001 | Yes | 6 tools — retrieval, order status, incidents, save-back, proactive monitoring queries |
| `services/orchestrator` | 8000 | Yes | Chat API, LangGraph agent, `/api/v1/internal/analyze_incident`, `/api/v1/projects` |
| `services/monitor/poller.py` | — | Yes, for proactive alerting | Background loop — polls every 10 min, dedups by `loggingId`, sends email/Slack |
| `services/ui/index.html` | 3000 | Optional (demo UI) | Project + persona picker, chat window |
| Kafka / `services/ingestor` | — | **No** (legacy) | Only relevant if you still want the old Kafka-based mock-log demo path |

---

## Prerequisites

- Docker Desktop 24.0+
- Python 3.11+
- Ollama with `qwen2.5:7b` and `qwen2.5:3b` pulled:
  ```powershell
  ollama pull qwen2.5:7b
  ollama pull qwen2.5:3b
  ```
- Real production OpenSearch access (host/port, and read credentials if security is enabled), **or** use the local test-data generator below to simulate it

---

## Step 1 — Start Infrastructure

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 up
```

This still brings up Kafka/Kafka UI too (docker-compose wasn't trimmed) — you can ignore those; nothing in the current flow depends on them. What matters: **OpenSearch, Postgres, Redis** report healthy.

```powershell
docker ps
```

---

## Step 2 — Install Python Dependencies

Three services matter now (plus the optional local-test generator):

```powershell
python -m pip install -r services\mcp-server\requirements.txt
python -m pip install -r services\orchestrator\requirements.txt
python -m pip install -r services\monitor\requirements.txt
python -m pip install -r services\log-generator\requirements.txt   # only if using generate_logs_opensearch.py
```

---

## Step 3 — Point at Your Project's Index

Open `services\mcp-server\project_config.py`. One project (`app_launchpad`) is
pre-wired to the index name from the client screenshot. To add more, or to
override the index name without touching code:

```powershell
$env:PROJECT_APP_LAUNCHPAD_INDEX = "your-real-index-alias"
$env:PROJECT_APP_LAUNCHPAD_SEARCH_FIELD = "msg"   # defaults to "msg" if unset
```

Add additional projects by editing the `PROJECTS` dict directly (each needs
`display_name`, `index`, `search_field`).

---

## Step 4 — Seed Historical Incidents (one-time)

```powershell
python scripts\seed_incidents.py
```

Creates `incidents-historical` with 8 curated seed RCAs (tagged
`source: "curated"`). The system writes new entries here automatically after
every real RCA (tagged `"auto-generated"` from chat, `"proactive-alert"`
from the poller) — you don't need to re-run this unless you want to reset
the curated baseline.

---

## Step 5 (Optional) — Seed Local Test Data

If you don't have access to a real production index yet, generate realistic
mock logs **directly into OpenSearch**, in the real field schema (no Kafka
needed):

```powershell
python services\log-generator\generate_logs_opensearch.py --transactions 100 --index fluentbit-csg-gr2v_app_launchpad-alias
```

This writes ~600 log lines across 10 failure scenarios (DB failures, NPEs,
payment timeouts, etc.), correlated by `loggingId`, matching the real
production shape. Skip this step entirely once you're pointed at a real
client index.

---

## Step 6 — Start the Application Services

Each runs in its own terminal.

**Terminal 1 — MCP Server**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload
```
Wait for `Application startup complete.`

**Terminal 2 — Orchestrator**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:7b"   # or qwen2.5:3b on low-RAM machines
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload
```
Wait for `Application startup complete.` and `✅ Redis connected.`

**Terminal 3 — Proactive Monitor (Module 3)**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\monitor
python poller.py
```
Wait for `✅ Redis connected. Polling every 600s.` Without `SMTP_HOST` /
`ALERT_EMAIL_DL` / `SLACK_WEBHOOK_URL` set, it still runs and logs detections
— it just skips the actual notification send (logged as a warning, not an
error). See `OPERATIONS_RUNBOOK_V2.md` for how to wire real credentials.

**Terminal 4 — UI (optional)**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000
```
Open **http://localhost:3000/index.html**.

---

## Step 7 — Verify

`scripts\smoke_test.py` is **stale** — it checks Kafka/k-NN/embedding-era
assumptions that no longer apply. Use these instead:

```powershell
# MCP server health + tool catalog
Invoke-RestMethod http://localhost:8001/healthz
Invoke-RestMethod http://localhost:8001/tools

# Orchestrator health + project list (what the UI's picker fetches)
Invoke-RestMethod http://localhost:8000/healthz
Invoke-RestMethod http://localhost:8000/api/v1/projects

# End-to-end chat query
$body = @{
  message = "Why did ORD-00005 fail?"
  session_id = "test"
  project_id = "app_launchpad"
  persona = "technical"
} | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```

Expect a JSON response with `evidence_count > 0` and, for a failing order,
`is_rca: true`. First query takes 30–60s while Ollama loads the model;
subsequent queries are 5–15s.

---

## Step 8 — Try the UI

1. Open http://localhost:3000/index.html
2. Pick a project from the greeting screen
3. Pick a persona — **Technical consultant** or **Business user**
4. Ask: `"Why did ORD-00005 fail?"` — compare the RCA tone if you try both personas in separate sessions ("New session" in the header)

---

## Stopping Everything

```powershell
.\run.ps1 down       # stop containers, keep data
.\run.ps1 nuke        # stop containers, delete all data (irreversible)
```

Ctrl+C in each of the 3–4 terminal windows to stop the Python services.

---

*See `OPERATIONS_RUNBOOK_V2.md` for restart scenarios, poller/notification troubleshooting, and day-2 operations.*


## Opensearch URL
http://localhost:5601/app/data-explorer/discover#?_a=(discover:(columns:!(log_message,exception),isDirty:!f,sort:!()),metadata:(indexPattern:'561d8020-8140-11f1-af70-25ef55e31390',view:discover))&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))&_q=(filters:!(),query:(language:kuery,query:''))
