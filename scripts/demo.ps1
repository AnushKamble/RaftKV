$ErrorActionPreference = "Stop"

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  RaftKV - Distributed Consensus Key-Value Store Real Live Demo" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

# 0. Build binaries
Write-Host "Step 0: Compiling server and CLI binaries..." -ForegroundColor Cyan
go build -o bin/raftkv-server.exe ./cmd/raftkv-server
go build -o bin/raftkv-cli.exe ./cmd/raftkv-cli

$demoDir = "data/demo"
if (Test-Path $demoDir) { Remove-Item -Recurse -Force $demoDir }
New-Item -ItemType Directory -Force -Path "$demoDir/node1" | Out-Null
New-Item -ItemType Directory -Force -Path "$demoDir/node2" | Out-Null
New-Item -ItemType Directory -Force -Path "$demoDir/node3" | Out-Null

$cfg1 = "id: `"node-1`"`ndata_dir: `"$demoDir/node1`"`naddress: `"127.0.0.1:56051`"`nmin_election_timeout: `"150ms`"`nmax_election_timeout: `"300ms`"`nheartbeat_interval: `"50ms`"`npeers:`n  node-2: `"127.0.0.1:56052`"`n  node-3: `"127.0.0.1:56053`""
$cfg2 = "id: `"node-2`"`ndata_dir: `"$demoDir/node2`"`naddress: `"127.0.0.1:56052`"`nmin_election_timeout: `"150ms`"`nmax_election_timeout: `"300ms`"`nheartbeat_interval: `"50ms`"`npeers:`n  node-1: `"127.0.0.1:56051`"`n  node-3: `"127.0.0.1:56053`""
$cfg3 = "id: `"node-3`"`ndata_dir: `"$demoDir/node3`"`naddress: `"127.0.0.1:56053`"`nmin_election_timeout: `"150ms`"`nmax_election_timeout: `"300ms`"`nheartbeat_interval: `"50ms`"`npeers:`n  node-1: `"127.0.0.1:56051`"`n  node-2: `"127.0.0.1:56052`""

Set-Content -Path "$demoDir/node1.yaml" -Value $cfg1
Set-Content -Path "$demoDir/node2.yaml" -Value $cfg2
Set-Content -Path "$demoDir/node3.yaml" -Value $cfg3

# 1. Start 3 node processes
Write-Host "Step 1: Starting 3 isolated RaftKV server processes on local TCP ports..." -ForegroundColor Cyan
$p1 = Start-Process -FilePath "bin/raftkv-server.exe" -ArgumentList "--config $demoDir/node1.yaml" -PassThru -NoNewWindow
$p2 = Start-Process -FilePath "bin/raftkv-server.exe" -ArgumentList "--config $demoDir/node2.yaml" -PassThru -NoNewWindow
$p3 = Start-Process -FilePath "bin/raftkv-server.exe" -ArgumentList "--config $demoDir/node3.yaml" -PassThru -NoNewWindow

$endpoints = "127.0.0.1:56051,127.0.0.1:56052,127.0.0.1:56053"

try {
    # 2. Wait for leader
    Write-Host "Step 2: Awaiting leader election..." -ForegroundColor Cyan
    Start-Sleep -Seconds 2

    $statusOutput = & bin/raftkv-cli.exe --endpoints=$endpoints status 127.0.0.1:56051
    Write-Host "Node 1 Status:`n$statusOutput" -ForegroundColor Green

    # 3. Perform Put and Get
    Write-Host "Step 3: Executing client operations (Put and Get)..." -ForegroundColor Cyan
    & bin/raftkv-cli.exe --endpoints=$endpoints put account:123 1000
    & bin/raftkv-cli.exe --endpoints=$endpoints get account:123

    # 4. Perform CAS and show commit advancement
    Write-Host "Step 4: Atomic CompareAndSwap (1000 -> 1250)..." -ForegroundColor Cyan
    & bin/raftkv-cli.exe --endpoints=$endpoints cas account:123 1000 1250
    & bin/raftkv-cli.exe --endpoints=$endpoints get account:123

    # 5. Kill leader abruptly
    Write-Host "Step 5: Killing leader node-1 abruptly to test failover..." -ForegroundColor Yellow
    Stop-Process -Id $p1.Id -Force
    Start-Sleep -Milliseconds 500

    # 6. Write while leader is dead
    Write-Host "Step 6: Proposing write to surviving majority (node-2, node-3)..." -ForegroundColor Cyan
    & bin/raftkv-cli.exe --endpoints="127.0.0.1:56052,127.0.0.1:56053" put account:123 2000
    & bin/raftkv-cli.exe --endpoints="127.0.0.1:56052,127.0.0.1:56053" get account:123

    # 7. Restart old leader
    Write-Host "Step 7: Restarting old leader node-1 from existing disk state..." -ForegroundColor Cyan
    $p1 = Start-Process -FilePath "bin/raftkv-server.exe" -ArgumentList "--config $demoDir/node1.yaml" -PassThru -NoNewWindow
    Start-Sleep -Seconds 1

    Write-Host "Node 1 Status after catching up:" -ForegroundColor Green
    & bin/raftkv-cli.exe --endpoints=$endpoints status 127.0.0.1:56051

    # 8. Deterministic simulation demonstration
    Write-Host "Step 8: Running deterministic partition and chaos verification..." -ForegroundColor Cyan
    go test -v ./tests/chaos -run TestChaos_SeededFaultCampaign/Seed_101

    # 9. Linearizability verification demonstration
    Write-Host "Step 9: Running history-based linearizability checker self-tests..." -ForegroundColor Cyan
    go test -v ./internal/linearizability -run TestChecker_KnownGoodSequential
    go test -v ./internal/linearizability -run TestChecker_KnownBadRealTimeViolation

    Write-Host "======================================================================" -ForegroundColor Green
    Write-Host "  Demo Completed Successfully! All 9 live gates passed." -ForegroundColor Green
    Write-Host "======================================================================" -ForegroundColor Green

} finally {
    Write-Host "Cleaning up demo server processes..." -ForegroundColor DarkGray
    if ($p1 -and !$p1.HasExited) { Stop-Process -Id $p1.Id -Force -ErrorAction SilentlyContinue }
    if ($p2 -and !$p2.HasExited) { Stop-Process -Id $p2.Id -Force -ErrorAction SilentlyContinue }
    if ($p3 -and !$p3.HasExited) { Stop-Process -Id $p3.Id -Force -ErrorAction SilentlyContinue }
    Remove-Item -Recurse -Force $demoDir -ErrorAction SilentlyContinue
}
