# ============================================================
# Meta Model Engine  Single Start Script
# Starts: Local Neo4j (Docker fallback) + Backend + Frontend
# Stop:   Press Ctrl+C  OR  click the Stop button in the UI
#         -> both cleanly kill all processes
# Usage:  .\start.ps1
# ============================================================

$Root        = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir  = Join-Path $Root "backend"
$FrontendDir = Join-Path $Root "frontend"
$_pipenvCmd  = Get-Command pipenv -ErrorAction SilentlyContinue
$Pipenv      = if ($_pipenvCmd) { $_pipenvCmd.Source } else { $null }

$script:BackendProc  = $null
$script:FrontendProc = $null
$script:Neo4jStarted = $false

#  Output helpers 
function Write-Header([string]$msg) {
    Write-Host ""
    Write-Host "=================================================" -ForegroundColor DarkCyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "=================================================" -ForegroundColor DarkCyan
}
function Write-Step([string]$msg)  { Write-Host "  >> $msg" -ForegroundColor Yellow }
function Write-OK([string]$msg)    { Write-Host "  [OK]   $msg" -ForegroundColor Green }
function Write-Fail([string]$msg)  { Write-Host "  [FAIL] $msg" -ForegroundColor Red }

#  Cleanup: kills everything 
function Stop-All {
    Write-Host ""
    Write-Header "Stopping all processes"

    if ($script:FrontendProc -and -not $script:FrontendProc.HasExited) {
        Stop-Process -Id $script:FrontendProc.Id -Force -ErrorAction SilentlyContinue
        Write-OK "Frontend stopped (PID $($script:FrontendProc.Id))"
    }

    if ($script:BackendProc -and -not $script:BackendProc.HasExited) {
        Stop-Process -Id $script:BackendProc.Id -Force -ErrorAction SilentlyContinue
        Write-OK "Backend stopped (PID $($script:BackendProc.Id))"
    }

    # Kill any stray uvicorn / node processes on those ports
    @(8000, 3000) | ForEach-Object {
        $port = $_
        $pid2 = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
                Select-Object -ExpandProperty OwningProcess -First 1
        if ($pid2) {
            Stop-Process -Id $pid2 -Force -ErrorAction SilentlyContinue
        }
    }

    if ($script:Neo4jStarted) {
        Write-Step "Stopping local Neo4j container..."
        docker compose stop neo4j-local 2>&1 | Out-Null
        Write-OK "Local Neo4j stopped"
    }

    Write-OK "All done. Goodbye."
}

#  Pre-flight checks 
Write-Header "Meta Model Engine  Starting"

Write-Step "Checking pipenv..."
if (-not $Pipenv) {
    Write-Fail "pipenv not found. Run: pip install pipenv"
    exit 1
}
Write-OK "pipenv found: $Pipenv"

Write-Step "Ensuring pipenv virtualenv is up to date..."
$env:PIPENV_VENV_IN_PROJECT = "1"
Set-Location $Root
pipenv sync 2>&1 | Out-Null
# pipenv may exit 1 even on success (stderr warnings); treat only code >1 as real failure
if ($LASTEXITCODE -gt 1) {
    Write-Fail "pipenv sync failed (exit $LASTEXITCODE)"
    exit 1
}
Write-OK "virtualenv ready"

Write-Step "Checking npm..."
$npmVer = npm --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Fail "npm not found. Install Node.js first."
    exit 1
}
Write-OK "npm $npmVer"

#  Free ports 
Write-Step "Freeing ports 3000 / 8000..."
@(3000, 8000) | ForEach-Object {
    $port = $_
    $pid2 = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -First 1
    if ($pid2) {
        $n = (Get-Process -Id $pid2 -ErrorAction SilentlyContinue).ProcessName
        if ($n -and $n -notmatch 'docker|vpnkit') {
            Stop-Process -Id $pid2 -Force -ErrorAction SilentlyContinue
            Write-Host "    Killed $n (PID $pid2) on port $port" -ForegroundColor DarkYellow
        }
    }
}

#  Local Docker Neo4j  started in background as AuraDB fallback only 
#  The backend connects to AuraDB first. Docker Neo4j is only used if AuraDB is unreachable.
#  No need to wait for it here — the backend health check confirms the actual DB connection.
Write-Step "Starting local Neo4j Docker (AuraDB fallback — background, no wait)..."

# Auto-launch Docker Desktop if it is installed but the engine is not yet running
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    $dockerDesktopExe = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    if (Test-Path $dockerDesktopExe) {
        Write-Step "Docker Desktop not running — launching it now (please wait up to 60s)..."
        Start-Process $dockerDesktopExe
        $dw = 0
        while ($dw -lt 60) {
            Start-Sleep -Seconds 5
            $dw += 5
            docker info 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) { Write-OK "Docker Desktop is ready"; break }
            Write-Host "    ... waiting for Docker ${dw}s" -ForegroundColor DarkGray
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  [WARN] Docker Desktop did not start in time  AuraDB-only mode" -ForegroundColor DarkYellow
        }
    }
}

$dockerOk = docker info 2>&1
if ($LASTEXITCODE -eq 0) {
    $running = docker ps --filter "name=meta_model_neo4j_local" --filter "status=running" -q 2>&1
    if (-not $running) {
        Set-Location $Root
        docker compose up neo4j-local -d 2>&1 | Out-Null
        Write-OK "Local Neo4j container started in background  (bolt://localhost:7687)"
    } else {
        Write-OK "Local Neo4j container already running  (bolt://localhost:7687)"
    }
    $script:Neo4jStarted = $true
} else {
    Write-Host "  [SKIP] Docker not available  AuraDB-only mode" -ForegroundColor DarkYellow
}

#  Start Backend 
Write-Step "Starting backend on http://localhost:8000 ..."
$env:PIPENV_VENV_IN_PROJECT = "1"
$script:BackendProc = Start-Process -NoNewWindow -PassThru `
    -FilePath $Pipenv `
    -ArgumentList "run","uvicorn","main:app","--app-dir","backend","--reload","--host","0.0.0.0","--port","8000" `
    -WorkingDirectory $Root
Write-OK "Backend PID: $($script:BackendProc.Id)"

# Wait for backend to be healthy
Write-Step "Waiting for backend health check..."
$waited = 0
$ready  = $false
while ($waited -lt 60) {
    Start-Sleep -Seconds 3
    $waited += 3
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
        if ($r.StatusCode -lt 500) { $ready = $true; break }
    } catch {}
    Write-Host "    ... waiting ${waited}s" -ForegroundColor DarkGray
}

if (-not $ready) {
    Write-Fail "Backend did not become healthy within 60s"
    Stop-All
    exit 1
}
Write-OK "Backend is healthy"

#  Start Frontend 
Write-Step "Starting frontend on http://localhost:3000 ..."
$script:FrontendProc = Start-Process -NoNewWindow -PassThru `
    -FilePath "npm" `
    -ArgumentList "run","dev" `
    -WorkingDirectory $FrontendDir
Write-OK "Frontend PID: $($script:FrontendProc.Id)"

# Wait for Vite to open port 3000 (TCP check — faster and more reliable than HTTP on dev servers)
Write-Step "Waiting for Vite to open port 3000..."
$feWaited = 0
$feReady  = $false
while ($feWaited -lt 60) {
    Start-Sleep -Seconds 2
    $feWaited += 2
    $tcp = Test-NetConnection -ComputerName localhost -Port 3000 -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
    if ($tcp.TcpTestSucceeded) { $feReady = $true; break }
}
if ($feReady) {
    Write-OK "Frontend is ready  (http://localhost:3000)"
} else {
    Write-Host "  [WARN] Vite port 3000 not detected after 60s — open http://localhost:3000 manually once Vite finishes compiling" -ForegroundColor DarkYellow
}

#  Ready 
Write-Header "All Services Running"
Write-Host ""
Write-Host "   Frontend  ->  http://localhost:3000" -ForegroundColor Cyan
Write-Host "   Backend   ->  http://localhost:8000" -ForegroundColor Cyan
Write-Host "   API Docs  ->  http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "   Neo4j     ->  bolt://localhost:7687  (local fallback)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Next steps:" -ForegroundColor Yellow
Write-Host "    1. Open http://localhost:3000" -ForegroundColor White
Write-Host "    2. Pipeline -> Run Hydration   (scans sample_repo — AST, cross-ref, schema, LLM, STM auto-seeds)" -ForegroundColor White
Write-Host "    3. Explore Lineage, Compliance, Impact, STM tabs once hydration completes" -ForegroundColor White
Write-Host ""
Write-Host "  To stop: press Ctrl+C here  OR  click the Stop button in the UI" -ForegroundColor DarkGray
Write-Host ""

#  Wait: exit when backend exits (triggered by /api/shutdown or Ctrl+C) 
try {
    # Block here  Wait-Process exits when the backend process ends
    $script:BackendProc | Wait-Process
} finally {
    # Always runs  whether UI clicked Stop, Ctrl+C, or backend crashed
    Stop-All
}
