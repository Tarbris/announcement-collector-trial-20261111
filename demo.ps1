param([int]$Port = 8765)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$Runtime = Join-Path $Root "runtime"
$Db = Join-Path $Runtime "demo.db"
$BaseUrl = "http://127.0.0.1:$Port"

New-Item -ItemType Directory -Force -Path $Runtime | Out-Null
foreach ($Path in @($Db, "$Db-wal", "$Db-shm")) {
    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Force
    }
}

$Server = Start-Process -FilePath "python" `
    -ArgumentList @("fixture_server.py", "--port", "$Port") `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -PassThru

try {
    $Ready = $false
    for ($Attempt = 0; $Attempt -lt 60; $Attempt++) {
        try {
            $Health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 2
            $Ready = $true
            break
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    if (-not $Ready) {
        throw "源服务未能在 6 秒内启动"
    }

    Write-Host "== clean first run =="
    python collector.py run --base-url $BaseUrl --db $Db --workers 2
    if ($LASTEXITCODE -ne 0) { throw "collector run failed" }

    Write-Host "== status =="
    python collector.py status --db $Db
    if ($LASTEXITCODE -ne 0) { throw "collector status failed" }

    Write-Host "== SQLite rows =="
    python dump_db.py --db $Db
    if ($LASTEXITCODE -ne 0) { throw "dump_db failed" }

    Write-Host "== source metrics before rerun =="
    $Before = Invoke-RestMethod -Uri "$BaseUrl/metrics"
    $Before | ConvertTo-Json -Depth 8

    Write-Host "== completed rerun (must not request /feed) =="
    python collector.py run --base-url $BaseUrl --db $Db --workers 2
    if ($LASTEXITCODE -ne 0) { throw "collector rerun failed" }

    Write-Host "== status after rerun =="
    python collector.py status --db $Db
    if ($LASTEXITCODE -ne 0) { throw "collector status after rerun failed" }

    Write-Host "== source metrics after rerun =="
    $After = Invoke-RestMethod -Uri "$BaseUrl/metrics"
    $After | ConvertTo-Json -Depth 8

    Write-Host "== feed request count before/after rerun =="
    Write-Host ("before={0} after={1} max_parallel={2}" -f $Before.requests.Count, $After.requests.Count, $After.max_parallel)
} finally {
    if ($Server -and -not $Server.HasExited) {
        Stop-Process -Id $Server.Id -Force
    }
}
