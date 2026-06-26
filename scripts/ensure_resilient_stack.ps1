param(
    [switch]$IncludeCollectors,
    [switch]$SkipXBrowser,
    [int]$SparkWorkers = 4
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvPath = Join-Path $ProjectRoot ".env"
Set-Location $ProjectRoot

function Test-DockerReady {
    docker info *> $null
    return $LASTEXITCODE -eq 0
}

function Start-DockerDesktopIfNeeded {
    if (Test-DockerReady) {
        return
    }

    $dockerDesktop = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
    if (Test-Path -LiteralPath $dockerDesktop) {
        Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
    }

    $deadline = (Get-Date).AddMinutes(3)
    while ((Get-Date) -lt $deadline) {
        if (Test-DockerReady) {
            return
        }
        Start-Sleep -Seconds 3
    }

    throw "Docker daemon is not reachable."
}

function Read-EnvFile {
    $values = @{}
    if (-not (Test-Path -LiteralPath $EnvPath)) {
        return $values
    }

    foreach ($line in Get-Content -LiteralPath $EnvPath) {
        if ($line -match '^\s*#' -or $line -notmatch '=') {
            continue
        }
        $name, $value = $line -split '=', 2
        $values[$name.Trim()] = $value.Trim()
    }
    return $values
}

function Set-EnvValue {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Value
    )

    $lines = @()
    if (Test-Path -LiteralPath $EnvPath) {
        $lines = @(Get-Content -LiteralPath $EnvPath)
    }

    $updated = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match "^\s*$([regex]::Escape($Name))=") {
            $lines[$i] = "$Name=$Value"
            $updated = $true
            break
        }
    }

    if (-not $updated) {
        $lines += "$Name=$Value"
    }

    Set-Content -LiteralPath $EnvPath -Value $lines -Encoding utf8
}

function Test-TcpPort {
    param(
        [string]$HostName = "127.0.0.1",
        [Parameter(Mandatory)][int]$Port
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $async = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(1500)) {
            return $false
        }
        $client.EndConnect($async)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
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

function Get-ComposePublishedPort {
    param(
        [Parameter(Mandatory)][string]$Service,
        [Parameter(Mandatory)][int]$ContainerPort
    )

    $output = docker compose port $Service $ContainerPort 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $output) {
        return $null
    }
    $line = @($output)[0]
    if ($line -match ':(\d+)$') {
        return [int]$Matches[1]
    }
    return $null
}

function Ensure-PortVariable {
    param(
        [hashtable]$EnvValues,
        [Parameter(Mandatory)][string]$Service,
        [Parameter(Mandatory)][int]$ContainerPort,
        [Parameter(Mandatory)][string]$Variable,
        [Parameter(Mandatory)][int]$DefaultPort
    )

    $publishedPort = Get-ComposePublishedPort -Service $Service -ContainerPort $ContainerPort
    if ($publishedPort) {
        Set-EnvValue -Name $Variable -Value ([string]$publishedPort)
        return $publishedPort
    }

    $desiredPort = $DefaultPort
    if ($EnvValues.ContainsKey($Variable) -and $EnvValues[$Variable] -match '^\d+$') {
        $desiredPort = [int]$EnvValues[$Variable]
    }

    if (Test-TcpPort -Port $desiredPort) {
        $desiredPort = Get-FreeTcpPort
        Write-Host "$Variable port was busy; using $desiredPort."
    }

    Set-EnvValue -Name $Variable -Value ([string]$desiredPort)
    return $desiredPort
}

function Wait-HttpEndpoint {
    param(
        [Parameter(Mandatory)][string]$Url,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5 | Out-Null
            return $true
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }
    return $false
}

function Get-EnvBool {
    param(
        [hashtable]$EnvValues,
        [string]$Name,
        [bool]$Default = $false
    )

    if (-not $EnvValues.ContainsKey($Name)) {
        return $Default
    }
    return $EnvValues[$Name].ToLowerInvariant() -in @("1", "true", "yes", "on")
}

function Ensure-XCdpProxy {
    param([hashtable]$EnvValues)

    if ($EnvValues.ContainsKey("X_CDP_URL") -and $EnvValues["X_CDP_URL"]) {
        return
    }

    $runtimePortPath = Join-Path $ProjectRoot "data\x-runtime\cdp-port.txt"
    $port = $null
    if (Test-Path -LiteralPath $runtimePortPath) {
        $rawPort = (Get-Content -LiteralPath $runtimePortPath -Raw).Trim()
        if ($rawPort -match '^\d+$') {
            $port = [int]$rawPort
        }
    }

    if ($port -and (Test-TcpPort -Port $port)) {
        return
    }

    $starter = Join-Path $PSScriptRoot "start_x_browser.ps1"
    powershell -ExecutionPolicy Bypass -File $starter
}

Start-DockerDesktopIfNeeded

$envValues = Read-EnvFile
$hostProjectDir = ($ProjectRoot -replace "\\", "/")
Set-EnvValue -Name "HOST_PROJECT_DIR" -Value $hostProjectDir
Set-EnvValue -Name "DOCKER_HOST_PROJECT_DIR" -Value $hostProjectDir

$ports = @(
    @{ Service = "minio"; ContainerPort = 9000; Variable = "MINIO_API_PORT"; DefaultPort = 9000 },
    @{ Service = "minio"; ContainerPort = 9001; Variable = "MINIO_CONSOLE_PORT"; DefaultPort = 9001 },
    @{ Service = "kafka"; ContainerPort = 9092; Variable = "KAFKA_HOST_PORT"; DefaultPort = 9092 },
    @{ Service = "schema-registry"; ContainerPort = 8081; Variable = "SCHEMA_REGISTRY_PORT"; DefaultPort = 8081 },
    @{ Service = "kafdrop"; ContainerPort = 9000; Variable = "KAFDROP_PORT"; DefaultPort = 9002 },
    @{ Service = "dashboard"; ContainerPort = 8501; Variable = "DASHBOARD_PORT"; DefaultPort = 8501 },
    @{ Service = "spark-master"; ContainerPort = 8080; Variable = "SPARK_MASTER_UI_PORT"; DefaultPort = 8080 },
    @{ Service = "spark-master"; ContainerPort = 7077; Variable = "SPARK_MASTER_PORT"; DefaultPort = 7077 },
    @{ Service = "airflow-webserver"; ContainerPort = 8080; Variable = "AIRFLOW_WEBSERVER_PORT"; DefaultPort = 8088 }
)

$resolvedPorts = @{}
foreach ($portSpec in $ports) {
    $resolvedPorts[$portSpec.Variable] = Ensure-PortVariable `
        -EnvValues $envValues `
        -Service $portSpec.Service `
        -ContainerPort $portSpec.ContainerPort `
        -Variable $portSpec.Variable `
        -DefaultPort $portSpec.DefaultPort
}

Set-EnvValue -Name "DASHBOARD_MINIO_ENDPOINT" -Value "http://localhost:$($resolvedPorts["MINIO_API_PORT"])"
Set-EnvValue -Name "DASHBOARD_AIRFLOW_URL" -Value "http://localhost:$($resolvedPorts["AIRFLOW_WEBSERVER_PORT"])"

$envValues = Read-EnvFile
if (-not $SkipXBrowser -and ($IncludeCollectors -or (Get-EnvBool -EnvValues $envValues -Name "X_COLLECTION_ENABLED"))) {
    Ensure-XCdpProxy -EnvValues $envValues
}

$coreServices = @(
    "minio",
    "minio-init",
    "kafka",
    "schema-registry",
    "kafdrop",
    "airflow-postgres",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "spark-master",
    "spark-worker",
    "dashboard"
)

& docker compose up -d --build --scale "spark-worker=$SparkWorkers" @coreServices

if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed."
}

$schemaOk = Wait-HttpEndpoint -Url "http://localhost:$($resolvedPorts["SCHEMA_REGISTRY_PORT"])/subjects" -TimeoutSeconds 120
$dashboardOk = Wait-HttpEndpoint -Url "http://localhost:$($resolvedPorts["DASHBOARD_PORT"])" -TimeoutSeconds 120
$minioOk = Wait-HttpEndpoint -Url "http://localhost:$($resolvedPorts["MINIO_API_PORT"])/minio/health/live" -TimeoutSeconds 120
$airflowOk = Wait-HttpEndpoint -Url "http://localhost:$($resolvedPorts["AIRFLOW_WEBSERVER_PORT"])/health" -TimeoutSeconds 420

if (-not ($schemaOk -and $dashboardOk -and $minioOk -and $airflowOk)) {
    throw "One or more core endpoints did not become reachable."
}

if ($IncludeCollectors) {
    & docker compose up -d --build youtube-collector x-collector reddit-collector
    if ($LASTEXITCODE -ne 0) {
        throw "collector startup failed."
    }
}

docker compose ps
Write-Host "Resolved ports were written to $EnvPath."
