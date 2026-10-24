param(
    [Parameter(Position=0)]
    [string]$Target = "test",
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"

function Run-Fmt {
    Write-Host "==> Formatting Go files..." -ForegroundColor Cyan
    go fmt ./...
}

function Run-Vet {
    Write-Host "==> Running go vet..." -ForegroundColor Cyan
    go vet ./...
}

function Run-Generate {
    Write-Host "==> Generating Protobuf / gRPC bindings..." -ForegroundColor Cyan
    if (!(Test-Path "gen/raftkv/v1")) {
        New-Item -ItemType Directory -Force -Path "gen/raftkv/v1" | Out-Null
    }
    protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/raftkv/v1/raftkv.proto
    Write-Host "Generated api/raftkv/v1 Protobuf bindings successfully." -ForegroundColor Green
}

function Run-Test {
    Write-Host "==> Running unit tests..." -ForegroundColor Cyan
    go test -v -count=1 ./...
}

function Run-Race {
    Write-Host "==> Running tests with data race detector..." -ForegroundColor Cyan
    go test -v -race -count=1 ./...
}

function Run-Integration {
    Write-Host "==> Running integration tests..." -ForegroundColor Cyan
    go test -v -count=1 ./tests/integration/...
}

function Run-Chaos {
    Write-Host "==> Running deterministic chaos tests..." -ForegroundColor Cyan
    go test -v -count=1 ./tests/chaos/...
}

function Run-Bench {
    Write-Host "==> Running benchmarks..." -ForegroundColor Cyan
    go test -v -bench=. -benchmem -run=^$ ./...
}

function Run-Clean {
    Write-Host "==> Cleaning build and temporary files..." -ForegroundColor Cyan
    $patterns = @("bin", "data", "tmp", "*.lock", "*.wal", "*.snap")
    foreach ($p in $patterns) {
        if (Test-Path $p) {
            Remove-Item -Recurse -Force $p -ErrorAction SilentlyContinue
        }
    }
    Write-Host "Clean completed." -ForegroundColor Green
}

switch ($Target) {
    "fmt" { Run-Fmt }
    "vet" { Run-Vet }
    "generate" { Run-Generate }
    "test" { Run-Test }
    "race" { Run-Race }
    "integration" { Run-Integration }
    "chaos" { Run-Chaos }
    "bench" { Run-Bench }
    "clean" { Run-Clean }
    "all" { Run-Fmt; Run-Vet; Run-Test }
    default {
        Write-Host "Unknown target: $Target. Supported: fmt, vet, generate, test, race, integration, chaos, bench, clean, all" -ForegroundColor Red
        exit 1
    }
}
