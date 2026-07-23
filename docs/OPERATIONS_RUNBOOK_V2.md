# GenAI RCA — Operations Runbook (Current Architecture)

This replaces `OPERATIONS_RUNBOOK.md` for the current build: production-index
retrieval (no ingestor/Kafka/embeddings), proactive error monitoring
(Module 3), and persona-aware, multi-project chat (Modules 4–5).

**Use this whenever:**
- You need to bring the system back up after a restart
- The proactive poller isn't alerting (or is alerting too much)
- You're onboarding a new project/index
- Something crashed and you need to recover

---

## Table of Contents

1. [Quick Status Check](#quick-status-check)
2. [Scenario A: Cold Start](#scenario-a-cold-start)
3. [Scenario B: Infra Up, Services Down](#scenario-b-infra-up-services-down)
4. [Scenario C: Partial Recovery](#scenario-c-partial-recovery)
5. [Scenario D: Clean Shutdown](#scenario-d-clean-shutdown)
6. [Scenario E: Wipe and Start Fresh](#scenario-e-wipe-and-start-fresh)
7. [Module 3 — Proactive Monitor Operations](#module-3--proactive-monitor-operations)
8. [Onboarding a New Project](#onboarding-a-new-project)
9. [Command Reference](#command-reference)
10. [Troubleshooting Each Layer](#troubleshooting-each-layer)
11. [What Lives Where](#what-lives-where)

---

## Quick Status Check

### Check 1: Is Docker Desktop running?

System tray whale icon: steady = up, animated = starting, absent = not running.

### Check 2: Are containers up?

```powershell
docker ps
```

You need **OpenSearch, Redis** healthy. Kafka/Kafka UI showing up too is
harmless — nothing in the current flow uses them. Postgres has been removed
(Phase 1 production elevation).

### Check 3: Is Ollama running?

```powershell
ollama list
```

### Check 4: Are application services running?

```powershell
Invoke-RestMethod http://localhost:8001/healthz    # MCP server
Invoke-RestMethod http://localhost:8000/healthz     # Orchestrator
```

`scripts\smoke_test.py` is **stale** (checks Kafka/k-NN/embedding-era
things) — don't rely on it. There's no dedicated healthcheck for the poller;
check its terminal/log output directly (it logs every poll cycle).

### Decision Tree

```
docker ps shows OpenSearch/Redis healthy?
├── YES → Are MCP server + orchestrator responding to /healthz?
│         ├── YES → Everything's up. Check the poller terminal separately.
│         └── NO  → Scenario B
└── NO  → Scenario A (cold start)
```

---

## Scenario A: Cold Start

**When**: Fresh after a restart, or you ran `down`/`nuke` last time.

### A1 — Start Docker Desktop
Wait for the steady whale icon.

### A2 — Start Ollama
Wait for the tray icon, verify with `ollama list`.

### A3 — Bring up infra
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 up
docker ps
```
Confirm OpenSearch and Redis are `Up (healthy)`.

### A4 — Verify data survived
```powershell
Invoke-RestMethod http://localhost:9200/incidents-historical/_count
```
Expect 8+ (grows over time as real RCAs get written back). If `count: 0`,
the volume was wiped — re-seed:
```powershell
python scripts\seed_incidents.py
```

Check your project's log index too (name from `project_config.py`):
```powershell
Invoke-RestMethod "http://localhost:9200/<your-index-name>/_count"
```
If it's empty and you're testing locally (not against a real client index):
```powershell
python services\log-generator\generate_logs_opensearch.py --transactions 100 --index <your-index-name>
```

### A5 — Start the 3 application services

**Window 1 — MCP Server**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload
```

**Window 2 — Orchestrator**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:7b"
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload
```
Wait for `✅ Redis connected.`

**Window 3 — Proactive Monitor**
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\monitor
python poller.py
```
Wait for `✅ Redis connected. Polling every 600s.`

### A6 — Verify
```powershell
Invoke-RestMethod http://localhost:8001/healthz
Invoke-RestMethod http://localhost:8000/healthz
Invoke-RestMethod http://localhost:8000/api/v1/projects
```

### A7 — Test query
```powershell
$body = @{ message = "Why did ORD-00005 fail?"; session_id = "demo"; project_id = "app_launchpad"; persona = "technical" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```
First query: 30–60s (Ollama model load). Subsequent: 5–15s.

### A8 — (Optional) Start the UI
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000
```
Browser → http://localhost:3000/index.html

---

## Scenario B: Infra Up, Services Down

**When**: Docker containers survived (still running), but you closed the
Python service terminals.

1. Verify Ollama: `ollama list`
2. Verify infra data intact: `Invoke-RestMethod http://localhost:9200/incidents-historical/_count`
3. Restart the 3 services — follow **A5** above
4. Verify — follow **A6/A7**

---

## Scenario C: Partial Recovery

| Symptom | Cause | Fix |
|---|---|---|
| MCP `/healthz` fails | Process died, or OpenSearch unreachable | Check `docker ps`; restart the MCP terminal (A5, Window 1) |
| Orchestrator `/healthz` fails | Process died, or MCP unreachable | Restart orchestrator terminal (A5, Window 2) |
| Poller logs "find_new_errors failed" repeatedly | MCP server down, or index name misconfigured | Check MCP is up; check `project_config.py` index name against `docker`'s OpenSearch |
| Chat returns `evidence_count: 0` for a known-bad order | Wrong `project_id`, or the order text doesn't appear in the configured `search_field` | Confirm `search_field` (default `msg`) actually contains the order number as text — see Troubleshooting below |
| Poller running but no alerts ever fire | No new `level:ERROR` logs since checkpoint, or `loggingId` field mapping mismatch | See [Module 3 Operations](#module-3--proactive-monitor-operations) |

To restart a single Docker container:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\infra
docker compose restart opensearch    # or redis
```

To restart a Python service: Ctrl+C in its terminal, then re-run the start
command from Scenario A5.

---

## Scenario D: Clean Shutdown

1. Ctrl+C in each of the 3–4 service terminals (MCP, orchestrator, poller, UI)
2. Stop infra, **preserving data**:
   ```powershell
   cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
   .\run.ps1 down
   ```
3. (Optional) Quit Ollama and/or Docker Desktop from the system tray

---

## Scenario E: Wipe and Start Fresh

⚠️ **Deletes all indexed logs, incidents, orders, poller checkpoints.**

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 nuke
```

Then follow **Scenario A** from the top, including re-seeding
`incidents-historical` and (if testing locally) your log index.

Note: wiping Redis also wipes the poller's checkpoints — on next start it
falls back to `INITIAL_LOOKBACK_HOURS` (default 1h) per project, so you
won't get a flood of alerts for old errors.

---

## Module 3 — Proactive Monitor Operations

### How it works, in one paragraph

Every `POLL_INTERVAL_SECONDS` (default 600 = 10 min), the poller asks MCP's
`find_new_errors` for each project's new `level:ERROR` logs since its last
checkpoint (stored in Redis), dedups them to one incident per distinct
`loggingId`, and for each new one calls the orchestrator's
`/api/v1/internal/analyze_incident` — which runs the exact same RCA
synthesis engine the chatbot uses — then emails the DL and posts to Slack.

### Checking what it's doing

The poller logs every cycle to its terminal:
```
2026-07-16 ... INFO ✅ Redis connected. Polling every 600s.
2026-07-16 ... INFO [app_launchpad] New error incident: loggingId=abc123 — Failed to obtain JDBC Connection...
2026-07-16 ... INFO ✅ Email alert sent to oncall-dl@example.com
2026-07-16 ... INFO ✅ Slack alert sent
2026-07-16 ... INFO [app_launchpad] Processed 1 new incident(s).
```

### Configuring notifications

Not required to run the poller — without these set, it logs a warning and
skips that channel:

```powershell
# Email
$env:SMTP_HOST = "smtp.yourcompany.com"
$env:SMTP_PORT = "587"
$env:SMTP_USER = "alerts@yourcompany.com"
$env:SMTP_PASSWORD = "..."
$env:SMTP_FROM = "genai-rca-alerts@yourcompany.com"
$env:ALERT_EMAIL_DL = "oncall-dl@yourcompany.com"

# Slack
$env:SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
```

Set these before starting `poller.py`. Restart the poller after changing
them (env vars are read once at process start).

### Tuning cadence and dedup

```powershell
$env:POLL_INTERVAL_SECONDS = "600"       # default 10 min, per requirement
$env:INITIAL_LOOKBACK_HOURS = "1"        # first-run window per project, avoids flooding on cold start
$env:ALERTED_TTL_SECONDS = "86400"       # how long a loggingId stays "already alerted" (24h default)
```

### "It's alerting too much" / "It's not alerting at all"

**Too much**: check `ALERTED_TTL_SECONDS` — if it's too short, the same
underlying issue can re-alert. Confirm dedup is keying correctly by
inspecting Redis:
```powershell
docker exec -it poc-redis redis-cli KEYS "monitor:alerted:*"
```

**Not at all**: check the checkpoint isn't stuck in the future, and that
`loggingId.keyword` actually exists on your index (see Troubleshooting):
```powershell
docker exec -it poc-redis redis-cli GET "monitor:checkpoint:app_launchpad"
```
To force a full re-scan for a project, delete its checkpoint key and
restart the poller:
```powershell
docker exec -it poc-redis redis-cli DEL "monitor:checkpoint:app_launchpad"
```

### Manually triggering a test alert

Rather than waiting for a real error, call the orchestrator's internal
endpoint directly with a known `loggingId` from your index:
```powershell
$body = @{ project_id = "app_launchpad"; logging_id = "trace-abc123" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/internal/analyze_incident -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```
This bypasses the poller's dedup entirely (useful for testing) — it will
run synthesis and attempt notification dispatch every time you call it.

---

## Onboarding a New Project

1. Confirm the real index name/alias and that it shares the same field
   schema (`msg`/`log_message`, `level`, `loggingId`, `exception`, `logger`,
   `instance`, `@timestamp`) as `app_launchpad`. If it differs, the
   retrieval and `find_new_errors` queries will need adjusting.
2. Add an entry to `services\mcp-server\project_config.py`:
   ```python
   "your_project_id": {
       "display_name": "Your Project Display Name",
       "index": os.getenv("PROJECT_YOUR_PROJECT_INDEX", "real-index-name"),
       "search_field": os.getenv("PROJECT_YOUR_PROJECT_SEARCH_FIELD", DEFAULT_SEARCH_FIELD),
   },
   ```
3. Restart the MCP server — the new project appears automatically in
   `GET /projects`, the UI's picker, and the poller's next cycle (it fetches
   the project list fresh every poll, no poller restart needed).
4. Verify: `Invoke-RestMethod http://localhost:8001/projects`

---

## Command Reference

### Docker
```powershell
docker ps
.\run.ps1 up
.\run.ps1 down
.\run.ps1 nuke
docker compose restart opensearch
docker logs poc-opensearch --tail 50
docker exec -it poc-redis redis-cli
```

### Application Services
```powershell
# MCP server
cd services\mcp-server
python -m uvicorn server:app --port 8001 --reload

# Orchestrator
cd services\orchestrator
python -m uvicorn main:app --port 8000 --reload

# Proactive monitor
cd services\monitor
python poller.py

# UI
cd services\ui
python -m http.server 3000

# Seed incidents (once, or to reset curated baseline)
python scripts\seed_incidents.py

# Local test data (no real index available)
python services\log-generator\generate_logs_opensearch.py --transactions 100 --index <index-name>
```

### Data Inspection
```powershell
# Count docs in a project's log index
Invoke-RestMethod "http://localhost:9200/<index-name>/_count"

# Count + breakdown of incidents-historical by source
$body = @{ size=0; aggs=@{ by_source=@{ terms=@{ field="source" } } } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://localhost:9200/incidents-historical/_search -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 6

# Find distinct loggingIds for an order (sanity check correlation)
$body = @{ query=@{ match=@{ msg="ORD-00005" } }; size=10 } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri "http://localhost:9200/<index-name>/_search" -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 6
```

### MCP Tool Direct Calls (bypass the LLM)
```powershell
# analyze_order_logs — default (last 24 hours)
$body = @{ project_id="app_launchpad"; order_no="ORD-00005"; top_k=3 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/analyze_order_logs -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# analyze_order_logs — scoped to a specific day (avoids token explosion for frequently-failing orders)
$body = @{ project_id="app_launchpad"; order_no="ORD-00005"; top_k=3; time_window_start="2026-07-07T00:00:00Z"; time_window_end="2026-07-07T23:59:59Z" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/analyze_order_logs -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# get_trace_by_logging_id
$body = @{ project_id="app_launchpad"; logging_id="trace-abc123" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/get_trace_by_logging_id -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# find_new_errors
$body = @{ project_id="app_launchpad"; since=(Get-Date).AddHours(-1).ToString("o") } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/find_new_errors -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# List all 5 tools
Invoke-RestMethod http://localhost:8001/tools
```

---

## Troubleshooting Each Layer

### `evidence_count` always 0

1. Confirm `project_id` in the request matches a key in `project_config.py`
2. Confirm the order number appears as literal text in the configured
   `search_field` (default `msg`) — if the real field is `log_message`
   instead, set `PROJECT_<ID>_SEARCH_FIELD` accordingly
3. Confirm the time window covers when the logs were written. By default the
   system looks back 24 hours. For older failures, include a date in the chat
   message: `"Why did ORD-00143 fail on 7th July?"` — the orchestrator
   extracts the date and narrows the OpenSearch range filter automatically
4. When calling `analyze_order_logs` directly (bypassing the chatbot), pass
   `time_window_start` / `time_window_end` as ISO datetimes if the logs fall
   outside the default 24-hour window

### `find_new_errors` / poller never finds anything

1. Confirm `loggingId.keyword` exists on the index — OpenSearch's dynamic
   "strings" template creates this automatically for `text` fields, but if
   the real mapping explicitly typed `loggingId` as `keyword` only (no
   multi-field), set `LOGGING_ID_FIELD=loggingId` (drop `.keyword`)
2. Confirm the checkpoint isn't in the future:
   `docker exec -it poc-redis redis-cli GET "monitor:checkpoint:<project_id>"`

### Orchestrator 500 on `/api/v1/chat`

1. Check the orchestrator terminal for the traceback
2. Most common: Ollama OOM — verify CPU mode:
   ```powershell
   [Environment]::GetEnvironmentVariable("OLLAMA_NUM_GPU", "User")   # should be 0
   ```
3. MCP unreachable — confirm `http://localhost:8001/healthz` responds

### Poller can't reach the orchestrator

Check `ORCHESTRATOR_BASE_URL` (default `http://localhost:8000`) and that
the orchestrator's `/api/v1/internal/analyze_incident` responds directly
(see the manual trigger command above).

### RCA write-back isn't showing up in `incidents-historical`

`save_incident` only fires when the LLM's response contains a `**Root
Cause**` section (i.e. it was a real RCA, not a happy-path/status summary).
Check the orchestrator log for `save_incident failed: ...` — a failure here
never blocks the user-facing response, so it can fail silently unless you're
watching the logs.

---

## What Lives Where

| What | Persists across restart? | Lost on `nuke`? |
|---|---|---|
| OpenSearch indices (logs, incidents) | Yes | Yes |
| Redis (poller checkpoints, alerted-loggingId set) | Yes | Yes |
| Ollama models | Yes (on disk) | No |
| Environment variables (User scope) | Yes | No |
| Code changes | Yes | No |

**Bottom line**: `down` is safe (keeps data), `nuke` is destructive
(including poller checkpoints — expect a brief re-scan window on next
start, bounded by `INITIAL_LOOKBACK_HOURS`).
