# =============================================================================
# GenAI RCA POC -- PowerShell runner (Windows equivalent of Makefile)
# =============================================================================
#
# Usage:
#     .\run.ps1 help              Show all commands
#     .\run.ps1 up                Bring up infrastructure
#     .\run.ps1 install           Install Python deps
#     .\run.ps1 ollama-pull       Pull required Ollama models
#     .\run.ps1 smoke             Verify all services
#     .\run.ps1 seed-incidents    Seed historical incidents
#     .\run.ps1 generate-logs     Generate mock logs to Kafka
#     .\run.ps1 ingest            Run the Kafka -> vector ingestor
#     .\run.ps1 mcp               Run the MCP server (port 8001)
#     .\run.ps1 orch              Run the orchestrator (port 8000)
#     .\run.ps1 ui                Serve the React UI (port 3000)
#     .\run.ps1 demo              Quick curl-style test
#     .\run.ps1 down              Stop infrastructure
#     .\run.ps1 nuke              Stop and DELETE all data
# =============================================================================

param(
    [Parameter(Position = 0)]
    [string]$Command = "help"
)

$ErrorActionPreference = "Stop"
$RepoRoot = $PSScriptRoot

# -----------------------------------------------------------------------------
# Simple output helpers (no emoji, no Unicode -- works on every Windows console)
# -----------------------------------------------------------------------------
function Write-Info($msg)    { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Success($msg) { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg)    { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)     { Write-Host "[FAIL] $msg" -ForegroundColor Red }

# -----------------------------------------------------------------------------
# Find Python: prefer 'py' (Windows Python launcher), fall back to 'python'
# -----------------------------------------------------------------------------
function Invoke-Python {
    param([string[]]$ScriptArgs)
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 @ScriptArgs
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        & python @ScriptArgs
    } else {
        Write-Err "Python not found. Install from https://www.python.org/downloads/"
        exit 1
    }
}

# -----------------------------------------------------------------------------
# Command implementations
# -----------------------------------------------------------------------------

function Cmd-Help {
    Write-Host ""
    Write-Host "GenAI RCA POC -- PowerShell commands:" -ForegroundColor White
    Write-Host ""
    Write-Host "  Infrastructure:" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 up               Bring up Kafka, OpenSearch, Postgres, Redis"
    Write-Host "    .\run.ps1 down             Stop infrastructure (preserves data)"
    Write-Host "    .\run.ps1 nuke             Stop and DELETE all data"
    Write-Host "    .\run.ps1 smoke            Verify all services are healthy"
    Write-Host ""
    Write-Host "  Setup (one-time):" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 install          Install Python deps for all services"
    Write-Host "    .\run.ps1 ollama-pull      Pull required Ollama models (about 6 GB)"
    Write-Host ""
    Write-Host "  Data:" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 seed-incidents   Seed historical incidents to OpenSearch"
    Write-Host "    .\run.ps1 generate-logs    Generate 100 mock transactions to Kafka"
    Write-Host "    .\run.ps1 generate-burst   Generate 1000 mock transactions fast"
    Write-Host ""
    Write-Host "  Run services (each in its own terminal):" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 ingest           Run the Kafka -> vector ingestor"
    Write-Host "    .\run.ps1 mcp              Run the MCP server (port 8001)"
    Write-Host "    .\run.ps1 orch             Run the orchestrator (port 8000)"
    Write-Host "    .\run.ps1 ui               Serve the React UI (port 3000)"
    Write-Host ""
    Write-Host "  Demo:" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 demo             Quick test: ask about ORD-00005"
    Write-Host ""
}

function Cmd-Up {
    Write-Info "Starting Docker infrastructure..."
    Push-Location "$RepoRoot\infra"
    try {
        docker compose up -d
        if ($LASTEXITCODE -ne 0) { throw "docker compose failed" }
        Write-Info "Waiting about 25 seconds for services to be healthy..."
        Start-Sleep -Seconds 25
        docker compose ps
        Write-Success "Infrastructure is up."
    } finally {
        Pop-Location
    }
}

function Cmd-Down {
    Push-Location "$RepoRoot\infra"
    try {
        docker compose down
        Write-Success "Stopped."
    } finally {
        Pop-Location
    }
}

function Cmd-Nuke {
    Write-Warn "This will DELETE ALL DATA. Confirm? (y/N)"
    $confirm = Read-Host
    if ($confirm -ne "y") {
        Write-Info "Aborted."
        return
    }
    Push-Location "$RepoRoot\infra"
    try {
        docker compose down -v
        Write-Success "Everything wiped."
    } finally {
        Pop-Location
    }
}

function Cmd-Smoke {
    Write-Info "Running smoke test..."
    Invoke-Python @("$RepoRoot\scripts\smoke_test.py")
}

function Cmd-Install {
    Write-Info "Installing Python dependencies for all services..."
    Write-Info "  - log-generator"
    Invoke-Python @("-m", "pip", "install", "-r", "$RepoRoot\services\log-generator\requirements.txt")
    Write-Info "  - ingestor"
    Invoke-Python @("-m", "pip", "install", "-r", "$RepoRoot\services\ingestor\requirements.txt")
    Write-Info "  - mcp-server"
    Invoke-Python @("-m", "pip", "install", "-r", "$RepoRoot\services\mcp-server\requirements.txt")
    Write-Info "  - orchestrator"
    Invoke-Python @("-m", "pip", "install", "-r", "$RepoRoot\services\orchestrator\requirements.txt")
    Write-Success "Dependencies installed."
}

function Cmd-OllamaPull {
    if (-not (Get-Command ollama -ErrorAction SilentlyContinue)) {
        Write-Err "Ollama not installed. Download from https://ollama.com"
        exit 1
    }
    Write-Info "Pulling qwen2.5:7b (about 4.7 GB)..."
    ollama pull qwen2.5:7b
    Write-Info "Pulling qwen2.5:3b (about 2 GB)..."
    ollama pull qwen2.5:3b
    Write-Success "Models pulled."
}

function Cmd-SeedIncidents {
    Write-Info "Seeding historical incidents into OpenSearch..."
    Invoke-Python @("$RepoRoot\scripts\seed_incidents.py")
}

function Cmd-GenerateLogs {
    Write-Info "Generating 100 mock transactions to Kafka..."
    Invoke-Python @("$RepoRoot\services\log-generator\generate_logs.py", "--transactions", "100", "--rate", "5")
}

function Cmd-GenerateBurst {
    Write-Info "Generating 1000 mock transactions (burst)..."
    Invoke-Python @("$RepoRoot\services\log-generator\generate_logs.py", "--transactions", "1000", "--burst")
}

function Cmd-Ingest {
    Write-Info "Starting Kafka -> embeddings -> OpenSearch ingestor..."
    Write-Warn "First run will download bge-large-en-v1.5 (about 1.3 GB). Be patient."
    Push-Location "$RepoRoot\services\ingestor"
    try {
        Invoke-Python @("ingestor.py")
    } finally {
        Pop-Location
    }
}

function Cmd-Mcp {
    Write-Info "Starting MCP server on http://localhost:8001 ..."
    Push-Location "$RepoRoot\services\mcp-server"
    try {
        Invoke-Python @("-m", "uvicorn", "server:app", "--port", "8001", "--reload")
    } finally {
        Pop-Location
    }
}

function Cmd-Orch {
    Write-Info "Starting orchestrator on http://localhost:8000 ..."
    Push-Location "$RepoRoot\services\orchestrator"
    try {
        Invoke-Python @("-m", "uvicorn", "main:app", "--port", "8000", "--reload")
    } finally {
        Pop-Location
    }
}

function Cmd-Ui {
    Write-Info "Serving React UI on http://localhost:3000/index.html ..."
    Push-Location "$RepoRoot\services\ui"
    try {
        Invoke-Python @("-m", "http.server", "3000")
    } finally {
        Pop-Location
    }
}

function Cmd-Demo {
    Write-Info "Asking: Why did ORD-00005 fail?"
    Write-Host ""
    $body = @{
        message    = "Why did ORD-00005 fail?"
        session_id = "demo"
    } | ConvertTo-Json
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/chat" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body
        $response | ConvertTo-Json -Depth 10
    } catch {
        Write-Err "Demo call failed: $_"
        Write-Warn "Make sure the orchestrator is running (.\run.ps1 orch)"
    }
}

# -----------------------------------------------------------------------------
# Dispatch
# -----------------------------------------------------------------------------
switch ($Command.ToLower()) {
    "help"             { Cmd-Help }
    "up"               { Cmd-Up }
    "down"             { Cmd-Down }
    "nuke"             { Cmd-Nuke }
    "smoke"            { Cmd-Smoke }
    "install"          { Cmd-Install }
    "ollama-pull"      { Cmd-OllamaPull }
    "seed-incidents"   { Cmd-SeedIncidents }
    "generate-logs"    { Cmd-GenerateLogs }
    "generate-burst"   { Cmd-GenerateBurst }
    "ingest"           { Cmd-Ingest }
    "mcp"              { Cmd-Mcp }
    "orch"             { Cmd-Orch }
    "ui"               { Cmd-Ui }
    "demo"             { Cmd-Demo }
    default {
        Write-Err "Unknown command: $Command"
        Cmd-Help
        exit 1
    }
}
