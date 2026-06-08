$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$profilePath = Join-Path $projectRoot "data\x-edge-profile"
$runtimePath = Join-Path $projectRoot "data\x-runtime"
$proxyScript = Join-Path $PSScriptRoot "x_cdp_proxy.py"
$proxyPidPath = Join-Path $projectRoot "data\x-cdp-proxy.pid"
$edgePidPath = Join-Path $projectRoot "data\x-edge.pid"
$edgeModePath = Join-Path $projectRoot "data\x-edge-mode.txt"
$proxyPortPath = Join-Path $runtimePath "cdp-port.txt"
$edgePortPath = Join-Path $runtimePath "edge-port.txt"
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
            Invoke-WebRequest `
                -Uri $Uri `
                -Headers $Headers `
                -UseBasicParsing `
                -TimeoutSec 3 | Out-Null
            return $true
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }

    return $false
}

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        0
    )
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Read-RuntimePort {
    param([Parameter(Mandatory)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $value = 0
    if (-not [int]::TryParse(
        (Get-Content -LiteralPath $Path -Raw).Trim(),
        [ref]$value
    )) {
        return $null
    }
    if ($value -lt 1 -or $value -gt 65535) {
        return $null
    }
    return $value
}

function Stop-ManagedProcess {
    param(
        [Parameter(Mandatory)][string]$PidPath,
        [Parameter(Mandatory)][string]$CommandPattern
    )

    if (-not (Test-Path -LiteralPath $PidPath)) {
        return
    }
    $managedPid = 0
    if (-not [int]::TryParse(
        (Get-Content -LiteralPath $PidPath -Raw).Trim(),
        [ref]$managedPid
    )) {
        Remove-Item -LiteralPath $PidPath -Force
        return
    }

    $process = Get-CimInstance Win32_Process `
        -Filter "ProcessId = $managedPid" `
        -ErrorAction SilentlyContinue
    if ($process -and $process.CommandLine -match $CommandPattern) {
        & taskkill.exe /PID $managedPid /T /F | Out-Null
    }
    Remove-Item -LiteralPath $PidPath -Force -ErrorAction SilentlyContinue
}

New-Item -ItemType Directory -Force -Path $profilePath, $runtimePath | Out-Null

$proxyPort = Read-RuntimePort -Path $proxyPortPath
if ($proxyPort) {
    $proxyUri = "http://127.0.0.1:$proxyPort/__x_cdp__/ensure?headless=false"
    if (Wait-HttpEndpoint `
        -Uri $proxyUri `
        -TimeoutSeconds 3 `
        -Headers @{ Host = "host.docker.internal:$proxyPort" }) {
        Write-Host "X browser and CDP proxy already ready on dynamic port $proxyPort."
        Write-Host "Log in at https://x.com before running the DAG."
        exit 0
    }
}

Remove-Item -LiteralPath $proxyPortPath, $edgePortPath `
    -Force `
    -ErrorAction SilentlyContinue
Stop-ManagedProcess `
    -PidPath $proxyPidPath `
    -CommandPattern ([regex]::Escape("x_cdp_proxy.py"))
Stop-ManagedProcess `
    -PidPath $edgePidPath `
    -CommandPattern ([regex]::Escape("x-edge-profile"))
# Edge can keep the profile lock briefly after its process tree exits.
Start-Sleep -Seconds 5

$edgePort = Get-FreeTcpPort
do {
    $proxyPort = Get-FreeTcpPort
} while ($proxyPort -eq $edgePort)

$edgeProcess = Start-Process -FilePath $edgePath -ArgumentList @(
    "--remote-debugging-address=127.0.0.1",
    "--remote-debugging-port=$edgePort",
    "--remote-allow-origins=*",
    "--user-data-dir=$profilePath",
    "--no-first-run",
    "--no-default-browser-check",
    "https://x.com/home"
) -PassThru
Set-Content -LiteralPath $edgePidPath -Value $edgeProcess.Id
Set-Content -LiteralPath $edgeModePath -Value "visible"

if (-not (Wait-HttpEndpoint -Uri "http://127.0.0.1:$edgePort/json/version")) {
    Remove-Item -LiteralPath $edgePidPath -Force -ErrorAction SilentlyContinue
    if ($edgeProcess.HasExited) {
        throw "Edge exited with code $($edgeProcess.ExitCode) before exposing CDP."
    }
    throw "Edge did not expose its CDP endpoint on dynamic port $edgePort."
}

$pythonPath = (Get-Command python -ErrorAction Stop).Source
$proxyProcess = Start-Process -FilePath $pythonPath -ArgumentList @(
    "-u",
    "`"$proxyScript`"",
    "--listen-port",
    "$proxyPort",
    "--target-port",
    "$edgePort",
    "--edge-path",
    "`"$edgePath`"",
    "--profile-path",
    "`"$profilePath`""
) -WindowStyle Hidden -PassThru
Set-Content -LiteralPath $proxyPidPath -Value $proxyProcess.Id

$proxyUri = "http://127.0.0.1:$proxyPort/__x_cdp__/ensure?headless=false"
if (-not (Wait-HttpEndpoint `
    -Uri $proxyUri `
    -Headers @{ Host = "host.docker.internal:$proxyPort" })) {
    throw "The X CDP proxy did not become ready on dynamic port $proxyPort."
}

Set-Content -LiteralPath $proxyPortPath -Value $proxyPort
Set-Content -LiteralPath $edgePortPath -Value $edgePort

Write-Host "X browser started with dynamic ports."
Write-Host "CDP proxy: $proxyPort; Edge CDP: $edgePort."
Write-Host "Log in at https://x.com before running the DAG."
