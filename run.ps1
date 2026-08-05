# =============================================================================
# GenAI RCA — Module 1 Runner
# =============================================================================
#
# Usage:
#     .\run.ps1 help              Show all commands
#     .\run.ps1 install           Install Python dependencies
#     .\run.ps1 server            Run Module 1 log retrieval server (port 8001)
#     .\run.ps1 check-opensearch  Verify OpenSearch cluster connection
#     .\run.ps1 demo              Test log retrieval API for sample order ORD-00042
# =============================================================================

param(
    [Parameter(Position = 0)]
    [string]$Command = "help"
)

$ErrorActionPreference = "Stop"
$RepoRoot = $PSScriptRoot

function Write-Info($msg)    { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Success($msg) { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg)    { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)     { Write-Host "[FAIL] $msg" -ForegroundColor Red }

function Get-PythonExe {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py"
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    } else {
        Write-Err "Python not found."
        exit 1
    }
}

$PythonExe = Get-PythonExe

switch ($Command.ToLower()) {
    "install" {
        Write-Info "Installing Module 1 requirements..."
        & $PythonExe -m pip install -r "$RepoRoot\services\mcp-server\requirements.txt"
        Write-Success "Dependencies installed."
    }
    "server" {
        Write-Info "Starting Module 1 Log Retrieval Server on port 8001..."
        Set-Location "$RepoRoot\services\mcp-server"
        & $PythonExe -m uvicorn server:app --port 8001 --reload
    }
    "check-opensearch" {
        Write-Info "Checking OpenSearch connectivity..."
        & $PythonExe "$RepoRoot\scripts\check_opensearch.py"
    }
    "demo" {
        Write-Info "Querying log retrieval endpoint for order ORD-00042..."
        try {
            $resp = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/logs?order_id=ORD-00042&project_id=app_launchpad" -Method Get
            $resp | ConvertTo-Json -Depth 5
        } catch {
            Write-Err "Failed to reach server. Make sure '.\run.ps1 server' is running."
        }
    }
    Default {
        Write-Host @"
GenAI RCA — Module 1 (Production Log Retrieval Layer)

Available Commands:
  .\run.ps1 install           Install Python dependencies
  .\run.ps1 server            Run log retrieval REST server (http://localhost:8001)
  .\run.ps1 check-opensearch  Verify OpenSearch connectivity
  .\run.ps1 demo              Test endpoint with sample order ORD-00042
"@ -ForegroundColor Yellow
    }
}
