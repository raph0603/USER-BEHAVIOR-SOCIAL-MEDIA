$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$profilePath = Join-Path $projectRoot "data\x-edge-profile"
$proxyScript = Join-Path $PSScriptRoot "x_cdp_proxy.py"
$proxyPidPath = Join-Path $projectRoot "data\x-cdp-proxy.pid"
$edgeCandidates = @(
    (Join-Path ${env:ProgramFiles(x86)} "Microsoft\Edge\Application\msedge.exe"),
    (Join-Path $env:ProgramFiles "Microsoft\Edge\Application\msedge.exe")
)
$edgePath = $edgeCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $edgePath) {
    throw "Microsoft Edge was not found."
}

function Wait-HttpEndpoint {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [int]$TimeoutSeconds = 45,
        [hashtable]$Headers = @{}
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec 3 | Out-Null
            return $true
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }

    return $false
}

New-Item -ItemType Directory -Force -Path $profilePath | Out-Null

if (-not (Wait-HttpEndpoint -Uri "http://127.0.0.1:9222/json/version" -TimeoutSeconds 3)) {
    if (Get-NetTCPConnection -LocalPort 9222 -State Listen -ErrorAction SilentlyContinue) {
        throw "Port 9222 is already in use by a process that is not an Edge CDP endpoint."
    }

    $edgeProcess = Start-Process -FilePath $edgePath -ArgumentList @(
        "--remote-debugging-address=127.0.0.1",
        "--remote-debugging-port=9222",
        "--remote-allow-origins=*",
        "--user-data-dir=$profilePath",
        "--no-first-run",
        "--no-default-browser-check",
        "https://x.com/home"
    ) -PassThru
}

if (-not (Wait-HttpEndpoint -Uri "http://127.0.0.1:9222/json/version")) {
    if ($edgeProcess -and $edgeProcess.HasExited) {
        throw "Edge exited with code $($edgeProcess.ExitCode) before exposing its CDP endpoint on port 9222."
    }

    throw "Edge did not expose its CDP endpoint on port 9222."
}

$proxyIsReady = Wait-HttpEndpoint `
    -Uri "http://127.0.0.1:9223/json/version" `
    -TimeoutSeconds 3 `
    -Headers @{ Host = "host.docker.internal:9223" }

if (-not $proxyIsReady) {
    if (Test-Path -LiteralPath $proxyPidPath) {
        $existingProxyPid = [int](Get-Content -LiteralPath $proxyPidPath)
        Stop-Process -Id $existingProxyPid -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }

    $pythonPath = (Get-Command python -ErrorAction Stop).Source
    $proxyProcess = Start-Process -FilePath $pythonPath -ArgumentList @(
        "-u",
        "`"$proxyScript`"",
        "--listen-port",
        "9223",
        "--target-port",
        "9222"
    ) -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath $proxyPidPath -Value $proxyProcess.Id
}

if (-not (Wait-HttpEndpoint `
    -Uri "http://127.0.0.1:9223/json/version" `
    -Headers @{ Host = "host.docker.internal:9223" })) {
    throw "The X CDP proxy did not become ready on port 9223."
}

Write-Host "X browser and CDP proxy started. Log in at https://x.com before running the DAG."
