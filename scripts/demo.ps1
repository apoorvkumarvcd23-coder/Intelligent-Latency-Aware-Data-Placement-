# =====================================================================
#  One-command demo (Windows / PowerShell).
#  Builds + starts the whole stack, waits for it to be healthy, opens the
#  dashboard, and tails the placement-engine log so you can narrate the demo.
#
#  Usage:   .\scripts\demo.ps1
#  Stop:    docker compose down         (add -v to also wipe data volumes)
# =====================================================================

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "==> Building and starting the stack (first run pulls images, be patient)..." -ForegroundColor Cyan
docker compose up -d --build

Write-Host "==> Waiting for the dashboard to come up at http://localhost:8501 ..." -ForegroundColor Cyan
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 3
        if ($resp.StatusCode -eq 200) { $ready = $true; break }
    } catch { Start-Sleep -Seconds 3 }
}

if ($ready) {
    Write-Host "==> Dashboard is up. Opening browser..." -ForegroundColor Green
    Start-Process "http://localhost:8501"
} else {
    Write-Host "!! Dashboard not reachable yet; it may still be starting. Check 'docker compose ps'." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Open these in your browser:" -ForegroundColor Cyan
Write-Host "   Dashboard (metrics) : http://localhost:8501"
Write-Host "   Spark UI            : http://localhost:4040"
Write-Host "   HDFS NameNode UI    : http://localhost:9870"
Write-Host ""
Write-Host "Tip: watch the dashboard. Every ~60s the workload's hot set shifts —"
Write-Host "     the cache-hit rate dips, then the engine re-learns and it recovers." -ForegroundColor Yellow
Write-Host ""
Write-Host "==> Tailing placement-engine logs (Ctrl+C to stop the log view; stack keeps running)..." -ForegroundColor Cyan
docker compose logs -f engine
