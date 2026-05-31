# GenAI RCA POC — Operations Runbook

Step-by-step commands for starting up, shutting down, and restarting the system.

**Use this whenever:**
- You closed your laptop and need to bring everything back up
- You restarted Windows
- You want to cleanly shut down at end of day
- Something crashed and you need to recover

---

## Table of Contents

1. [Quick Status Check — Where Am I?](#quick-status-check--where-am-i)
2. [Scenario A: Cold Start (Everything Down)](#scenario-a-cold-start-everything-down)
3. [Scenario B: Docker Already Up, Services Down](#scenario-b-docker-already-up-services-down)
4. [Scenario C: Partial Recovery (Some Things Up, Some Down)](#scenario-c-partial-recovery-some-things-up-some-down)
5. [Scenario D: Clean Shutdown (End of Day)](#scenario-d-clean-shutdown-end-of-day)
6. [Scenario E: Wipe and Start Fresh](#scenario-e-wipe-and-start-fresh)
7. [Command Reference](#command-reference)
8. [Troubleshooting Each Layer](#troubleshooting-each-layer)
9. [What Lives Where (After Closing Windows)](#what-lives-where-after-closing-windows)

---

## Quick Status Check — Where Am I?

Before doing anything, find out what's running. Open a PowerShell window and run these in order:

### Check 1: Is Docker Desktop running?

Look at the **system tray** (bottom-right of screen, near the clock). Find the **whale icon**:
- **Steady whale** → Docker is up
- **Animated whale** → Docker is starting (wait 1-2 min)
- **No whale icon** → Docker Desktop is not running

If no whale icon: open Docker Desktop from Start menu, wait until the icon appears steady.

### Check 2: Are Docker containers running?

```powershell
docker ps
```

**Expected output if all containers are up:**
```
CONTAINER ID   IMAGE                                            STATUS         PORTS                              NAMES
xxxxxxx        confluentinc/cp-kafka:7.7.1                      Up (healthy)   0.0.0.0:9092->9092/tcp, ...        poc-kafka
xxxxxxx        opensearchproject/opensearch:2.18.0              Up (healthy)   0.0.0.0:9200->9200/tcp, ...        poc-opensearch
xxxxxxx        provectuslabs/kafka-ui:latest                    Up             0.0.0.0:8090->8080/tcp             poc-kafka-ui
xxxxxxx        opensearchproject/opensearch-dashboards:2.18.0   Up             0.0.0.0:5601->5601/tcp             poc-osd
xxxxxxx        postgres:16-alpine                               Up (healthy)   0.0.0.0:5432->5432/tcp             poc-postgres
xxxxxxx        redis:7-alpine                                   Up (healthy)   0.0.0.0:6379->6379/tcp             poc-redis
```

- **6 containers shown** → Skip Docker setup; go to Scenario B
- **Empty list / just headers** → Containers are stopped; use Scenario A

### Check 3: Is Ollama running?

```powershell
ollama list
```

- **Shows models (qwen2.5:7b, qwen2.5:3b, etc.)** → Ollama is up
- **"command not found" or empty** → Need to start Ollama from Start menu

### Check 4: Are application services running?

```powershell
python C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\scripts\smoke_test.py
```

This single command tells you the status of every layer. Look at the bottom — if you see "All checks passed (7/7)", everything is up. Otherwise, the specific WARN/FAIL lines tell you what needs starting.

### Decision Tree

```
docker ps shows 6 containers?
├── YES → Are Python services running?
│         ├── YES (smoke test shows 7/7)  → Everything is up. Just open new windows to query.
│         └── NO  (smoke shows MCP/Orchestrator WARN) → Go to Scenario B
└── NO  → Go to Scenario A (cold start)
```

---

## Scenario A: Cold Start (Everything Down)

**When to use**: Fresh after PC restart, or you ran `down` last time, or all PowerShell windows are closed and Docker isn't running.

**Time required**: ~5-10 minutes total.

### Step A1: Start Docker Desktop

1. Press **Windows key** → type **Docker Desktop** → press Enter
2. Wait for the whale icon in the system tray to be **steady** (not animated)
3. This usually takes 30-60 seconds

**Verify:**
```powershell
docker info
```
Should print a long block of info. If you see "Cannot connect to the Docker daemon", wait another 30 seconds and retry.

### Step A2: Start Ollama (if not already running)

1. Check system tray for the **llama icon** (looks like a llama outline)
2. If missing: press **Windows key** → type **Ollama** → press Enter
3. Wait for the tray icon to appear (~10 seconds)

**Verify:**
```powershell
ollama list
```
Should show your models including `qwen2.5:3b` and `qwen2.5:7b`.

### Step A3: Bring Up Docker Infrastructure

Open PowerShell:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 up
```

**Wait for** (~30 seconds):
```
NAME              STATUS
poc-kafka         Up X seconds (healthy)
poc-kafka-ui      Up X seconds
poc-opensearch    Up X seconds (healthy)
poc-osd           Up X seconds
poc-postgres      Up X seconds (healthy)
poc-redis         Up X seconds (healthy)
[ OK ] Infrastructure is up.
```

### Step A4: Verify Infrastructure

In the same window:
```powershell
python scripts\smoke_test.py
```

First 6 checks should be green (Kafka, OpenSearch, k-NN, Postgres, Redis, Ollama). MCP and Orchestrator will show WARN — that's expected.

### Step A5: Verify Data Survived

```powershell
Invoke-RestMethod http://localhost:9200/logs-vectors-current/_count
Invoke-RestMethod http://localhost:9200/incidents-historical/_count
```

**Expected:**
- `logs-vectors-current` count: ~100 (whatever you had before)
- `incidents-historical` count: 8

**If both show `count: 0`** → Docker volumes were wiped. Re-seed:
```powershell
python scripts\seed_incidents.py
python services\log-generator\generate_logs.py --transactions 100 --rate 5
```

### Step A6: Start the 3 Application Services

**You need 3 separate PowerShell windows.** Each runs one service and stays open.

#### Window 1 — Ingestor

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python services\ingestor\ingestor.py
```

**Wait for:**
```
INFO ✅ Consumer started on topic=app-logs group=genai-ingestor-cg
```

**Leave this window open.** Don't click inside it (QuickEdit freeze risk).

#### Window 2 — MCP Server

Open a **NEW** PowerShell window:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload
```

**Wait for:**
```
INFO:     Application startup complete.
```

**Leave this window open.**

#### Window 3 — Orchestrator

Open another **NEW** PowerShell window:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:3b"
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload
```

**Wait for:**
```
INFO:     Application startup complete.
2026-05-22 XX:XX:XX INFO ✅ Redis connected.
```

**Leave this window open.**

### Step A7: Final Verification

Open a **NEW** (4th) PowerShell — this is your "command" window:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python scripts\smoke_test.py
```

**Expected:** All 7 (or 9) checks green.

### Step A8: Run a Test Query

```powershell
$body = @{ message = "Why did ORD-00175 fail?"; session_id = "demo" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```

**First query takes 30-60 sec** (Ollama loading the model). Should return a JSON with `evidence_count: 1` and a full RCA.

### Step A9: (Optional) Start the UI

Open a **5th** PowerShell window:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000
```

Open browser → **http://localhost:3000/index.html**

---

## Scenario B: Docker Already Up, Services Down

**When to use**: You closed all PowerShell windows, but `docker ps` shows the 6 containers still running.

**Time required**: ~2 minutes.

### Step B1: Verify Ollama Is Still Running

```powershell
ollama list
```

If models are listed → Ollama is fine. If not, see Step A2 to start it.

### Step B2: Verify Data Is Intact

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python scripts\smoke_test.py
```

First 6 checks should be green. (Last 2 will be WARN — those are the services we need to start.)

### Step B3: Start the 3 Application Services

Follow **Step A6** above. You need 3 separate PowerShell windows for ingestor, MCP server, and orchestrator.

### Step B4: Verify & Test

Follow **Step A7** and **A8** above.

---

## Scenario C: Partial Recovery (Some Things Up, Some Down)

**When to use**: Smoke test shows mixed results — some services up, some down. Maybe one service crashed.

### Step C1: Identify What's Broken

Run the smoke test:
```powershell
python C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\scripts\smoke_test.py
```

Look at the output:

| What you see | What it means | What to do |
|---|---|---|
| ❌ Kafka unreachable | Kafka container down | `docker compose -f infra/docker-compose.yml up -d kafka` |
| ❌ OpenSearch unreachable | OpenSearch container down | `docker compose -f infra/docker-compose.yml up -d opensearch` |
| ❌ Postgres unreachable | Postgres container down | `docker compose -f infra/docker-compose.yml up -d postgres` |
| ❌ Redis unreachable | Redis container down | `docker compose -f infra/docker-compose.yml up -d redis` |
| ❌ Ollama unreachable | Ollama service stopped | Start from Start menu |
| ⚠️ MCP server not running | Process died | Restart in Window 2 (Step A6) |
| ⚠️ Orchestrator not running | Process died | Restart in Window 3 (Step A6) |

### Step C2: Restart Specific Components

#### To restart a single Docker container

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\infra
docker compose restart <service-name>
```

Where `<service-name>` is one of: `kafka`, `opensearch`, `postgres`, `redis`, `kafka-ui`, `opensearch-dashboards`

Example:
```powershell
docker compose restart opensearch
```

#### To restart a Python service

Each Python service runs in its own PowerShell window. Find that window, press **Ctrl+C**, wait for the prompt, then re-run the start command from Step A6.

If the window is closed entirely, open a new one and follow Step A6 for the appropriate service.

---

## Scenario D: Clean Shutdown (End of Day)

**When to use**: Done for the day, want everything off, don't want to wipe data.

### Step D1: Stop Python Services

In each of your 3+ service windows, press **Ctrl+C** to stop the service. Wait a few seconds for clean shutdown. You can close the PowerShell windows after.

### Step D2: Stop Docker Containers

In any PowerShell:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 down
```

Or directly:
```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\infra
docker compose down
```

**Important**: This stops containers but **preserves volumes** (your data). Tomorrow when you start up, all your indexed logs, orders, and incidents will still be there.

### Step D3: (Optional) Stop Ollama

If you want to stop Ollama entirely (frees ~2 GB RAM if a model was loaded):
1. Right-click Ollama llama icon in system tray
2. Click **Quit Ollama**

You don't have to do this — Ollama uses minimal resources when idle.

### Step D4: (Optional) Stop Docker Desktop

If you want to stop Docker Desktop entirely:
1. Right-click Docker whale icon in system tray
2. Click **Quit Docker Desktop**

---

## Scenario E: Wipe and Start Fresh

**When to use**: You want to reset everything to a clean state. Lose all data.

⚠️ **WARNING**: This deletes ALL your indexed logs, orders, incidents, embeddings. You'll have to re-seed everything from scratch.

### Step E1: Stop and Delete Everything

```powershell
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 nuke
```

It will ask for confirmation. Press **y** + Enter.

This:
- Stops all containers
- Deletes all Docker volumes (Kafka data, OpenSearch indices, Postgres tables, Redis data)
- Removes container metadata

### Step E2: Bring Everything Back Up

Follow **Scenario A** (Cold Start) from the beginning. You'll need to re-seed data:
```powershell
python scripts\seed_incidents.py
python services\log-generator\generate_logs.py --transactions 100 --rate 5
```

---

## Command Reference

### Docker Commands

```powershell
# Check what containers are running
docker ps

# Check all containers (including stopped ones)
docker ps -a

# Start all containers (preserves data)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 up

# Stop all containers (preserves data)
.\run.ps1 down

# Wipe everything (loses data)
.\run.ps1 nuke

# Restart a specific container
cd infra
docker compose restart kafka
docker compose restart opensearch
docker compose restart postgres
docker compose restart redis

# View container logs
docker logs poc-kafka --tail 50
docker logs poc-opensearch --tail 50
docker logs poc-postgres --tail 50

# Exec into a container
docker exec -it poc-postgres psql -U postgres -d orders
docker exec -it poc-kafka /bin/bash
```

### Python Service Commands

```powershell
# Start ingestor
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python services\ingestor\ingestor.py

# Start MCP server (port 8001)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload

# Start orchestrator (port 8000)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:3b"
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload

# Start React UI (port 3000)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000

# Run smoke test
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
python scripts\smoke_test.py

# Generate more mock logs
python services\log-generator\generate_logs.py --transactions 100 --rate 5

# Generate logs in burst mode (fast)
python services\log-generator\generate_logs.py --transactions 500 --burst

# Seed historical incidents (only needed first time)
python scripts\seed_incidents.py
```

### Data Inspection Commands

```powershell
# Count vectors in OpenSearch
Invoke-RestMethod http://localhost:9200/logs-vectors-current/_count

# Count historical incidents
Invoke-RestMethod http://localhost:9200/incidents-historical/_count

# Get all unique order numbers in the vector index
$body = @{ size = 0; aggs = @{ unique_orders = @{ terms = @{ field = "order_no"; size = 30 } } } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://localhost:9200/logs-vectors-current/_search -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 6

# Search OpenSearch for a specific order
$body = @{ query = @{ term = @{ order_no = "ORD-00175" } } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://localhost:9200/logs-vectors-current/_search -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5

# Query Postgres
docker exec -it poc-postgres psql -U postgres -d orders -c "SELECT order_no, status, failed_step FROM orders WHERE status='FAILED' LIMIT 10;"
docker exec -it poc-postgres psql -U postgres -d orders -c "SELECT status, COUNT(*) FROM orders GROUP BY status;"
```

### MCP Tool Direct Calls (Test Without LLM)

```powershell
# Tool 1: analyze_order_logs
$body = @{ order_no = "ORD-00175"; top_k = 5 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/analyze_order_logs -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 4

# Tool 2: get_order_status
$body = @{ order_no = "ORD-00005" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/get_order_status -Method Post -ContentType "application/json" -Body $body

# Tool 3: find_similar_incidents
$body = @{ description = "payment failure"; top_k = 3 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8001/tools/find_similar_incidents -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 3

# List all tools
Invoke-RestMethod http://localhost:8001/tools
```

### Orchestrator / Demo Commands

```powershell
# Run an RCA query
$body = @{ message = "Why did ORD-00175 fail?"; session_id = "demo" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5

# Status-only query
$body = @{ message = "What is the status of ORD-00100?"; session_id = "demo" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
```

### Ollama Commands

```powershell
# List models
ollama list

# Show running models (loaded into RAM)
ollama ps

# Test a model directly
ollama run qwen2.5:3b "hello"

# Stop a model (unload from RAM)
ollama stop qwen2.5:3b

# Pull a model
ollama pull qwen2.5:3b
```

### Browser URLs

| Service | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8090 | Browse topics, messages |
| OpenSearch | http://localhost:9200 | Query API |
| OpenSearch Dashboards | http://localhost:5601 | Visual OpenSearch UI |
| MCP Server | http://localhost:8001/tools | Tool catalog |
| Orchestrator | http://localhost:8000/healthz | Health check |
| React UI | http://localhost:3000/index.html | Chatbot |

---

## Troubleshooting Each Layer

### Docker Won't Start

**Symptom**: `docker info` errors with "Cannot connect to the Docker daemon"

**Fix**:
1. Open Docker Desktop manually (Start menu)
2. If it crashes, restart Windows
3. If still failing, check WSL2: `wsl --status`

### Container Keeps Restarting

**Symptom**: `docker ps` shows a container in "Restarting" loop

**Fix**:
```powershell
docker logs poc-opensearch --tail 100
```

Common causes:
- OpenSearch out of memory → increase Docker Desktop memory to 6 GB
- Postgres init.sql errors → check `docker logs poc-postgres`

### Ingestor Stops Indexing

**Symptom**: Ingestor window shows last log but no new "Indexed N chunks" lines

**Causes & Fixes**:
- **QuickEdit Mode**: you clicked inside the window. Press Enter to unfreeze.
- **No new messages in Kafka**: generate more logs:
  ```powershell
  python services\log-generator\generate_logs.py --transactions 50 --rate 10
  ```
- **Crashed silently**: check the window for errors. Restart it (Step A6, Window 1).

### MCP Server "Internal Server Error"

**Symptom**: HTTP 500 from MCP endpoints

**Fix**:
1. Check the MCP server window for the error traceback
2. Common: OpenSearch unreachable → check `docker ps`
3. If embedding model fails to load → free up RAM and restart MCP

### Orchestrator "Internal Server Error"

**Symptom**: HTTP 500 from `/api/v1/chat`

**Fix**:
1. Check orchestrator window for traceback
2. Most common: Ollama OOM → ensure CPU mode is set:
   ```powershell
   [Environment]::GetEnvironmentVariable("OLLAMA_NUM_GPU", "User")
   # Should print: 0
   ```
3. If not set, set it and restart Ollama:
   ```powershell
   [Environment]::SetEnvironmentVariable("OLLAMA_NUM_GPU", "0", "User")
   ```

### evidence_count Always 0

**Symptom**: RCA responses say "no logs found" even with indexed data

**Fix**: Check `services\mcp-server\server.py` — the `analyze_order_logs` function should use a simple `term` filter on `order_no`. If it has complex bool queries with knn inside should clauses, replace it with the simpler version (filter-only).

### Smoke Test Says "All packages OK" but Service Won't Start

**Symptom**: Imports work but uvicorn fails with `ModuleNotFoundError`

**Fix**: You're not in the right directory. uvicorn needs to be in the service's folder:
- For MCP: `cd services\mcp-server` then run uvicorn
- For orchestrator: `cd services\orchestrator` then run uvicorn

---

## What Lives Where (After Closing Windows)

When you close PowerShell windows or shut down your PC:

| What | Status | Persistence |
|---|---|---|
| Docker images | Stay on disk | Forever, until `docker rmi` or Docker Desktop reset |
| Docker volumes (OpenSearch indices, Postgres tables, Kafka messages, Redis) | Persist if `down` was used | Lost only with `nuke` or `docker volume prune` |
| Python packages | Installed system-wide | Persist forever |
| Ollama models | Stay on disk (`~/.ollama/`) | Persist forever |
| Embedding model cache (`~/.cache/huggingface/`) | Stays on disk | Persist forever |
| Environment variables (OLLAMA_NUM_GPU, etc.) | "User" scope = permanent | Persist across reboots |
| Code changes | On disk | Persist forever |

**The only ephemeral things are the running processes** (ingestor, MCP, orchestrator) and what's loaded in RAM (LLM models, Python interpreters).

**Bottom line**: Closing windows or restarting your PC doesn't destroy data. You just need to restart the services. That's what this runbook is for.

---

## Recommended Daily Workflow

### Start of day (PC was off)

1. Wait for PC to boot
2. Open Docker Desktop (if not auto-starting)
3. Wait for Ollama tray icon (if not auto-starting)
4. Open PowerShell, run `cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc`
5. Run `.\run.ps1 up`
6. Open 3 more PowerShell windows for ingestor, MCP, orchestrator
7. Run `python scripts\smoke_test.py` to verify

### End of day

1. In each service window, press Ctrl+C to stop the service
2. Run `.\run.ps1 down` in any PowerShell
3. Close PowerShell windows

### After a crash or restart

1. Run `docker ps` to see what's still up
2. Use the [Decision Tree](#decision-tree) above to pick Scenario A, B, or C
3. Follow the steps for that scenario

---

## Quick Card — Most Common Commands

Print this out or keep it nearby:

```powershell
# ────────────────────────────────────────────────────────────
# COLD START (everything off)
# ────────────────────────────────────────────────────────────
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 up                                  # Docker
python scripts\smoke_test.py                  # verify

# Window 1: Ingestor
python services\ingestor\ingestor.py

# Window 2: MCP (open new PowerShell)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\mcp-server
python -m uvicorn server:app --port 8001 --reload

# Window 3: Orchestrator (open new PowerShell)
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\orchestrator
$env:SYNTHESIS_MODEL = "qwen2.5:3b"
$env:ROUTER_MODEL = "qwen2.5:3b"
python -m uvicorn main:app --port 8000 --reload

# Window 4: Commands / Demo
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
$body = @{ message = "Why did ORD-00175 fail?"; session_id = "demo" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/api/v1/chat -Method Post -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5

# Optional Window 5: UI
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc\services\ui
python -m http.server 3000
# Browser: http://localhost:3000/index.html

## To run in powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

## Stop service at 8080
Stop-Service -Name "Jenkins"

# ────────────────────────────────────────────────────────────
# END OF DAY
# ────────────────────────────────────────────────────────────
# Ctrl+C in each service window, then:
cd C:\Mamidi\2026\genai-rca-poc\genai-rca-poc
.\run.ps1 down
```

---

*Save this file. Print the Quick Card. Bookmark it. You'll thank yourself.*
