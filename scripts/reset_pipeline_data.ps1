[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$rootPrefix = $projectRoot.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "docker-compose.yml was not found under $projectRoot"
}

$dataTargets = @(
    "data\minio",
    "data\collector-state",
    "data\collectors",
    "API\yt_raw_json"
)

function Get-SafeTargetPath {
    param([Parameter(Mandatory)][string]$RelativePath)

    $fullPath = [System.IO.Path]::GetFullPath((Join-Path $projectRoot $RelativePath))
    if (-not $fullPath.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clear a path outside the project: $fullPath"
    }

    return $fullPath
}

function Clear-DirectoryContents {
    param([Parameter(Mandatory)][string]$RelativePath)

    $targetPath = Get-SafeTargetPath -RelativePath $RelativePath
    New-Item -ItemType Directory -Force -Path $targetPath | Out-Null
    $targetPrefix = $targetPath.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar

    foreach ($item in Get-ChildItem -LiteralPath $targetPath -Force) {
        $itemPath = [System.IO.Path]::GetFullPath($item.FullName)
        if (-not $itemPath.StartsWith($targetPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to clear an unexpected path: $itemPath"
        }

        # OneDrive marks both files and directories as reparse points. Windows
        # PowerShell needs -Recurse to remove those placeholders reliably.
        Remove-Item -LiteralPath $itemPath -Recurse -Force
    }

    Write-Host "Cleared $RelativePath"
}

function Invoke-Compose {
    param([Parameter(Mandatory)][string[]]$Arguments)

    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Wait-HttpEndpoint {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$Name,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 3 | Out-Null
            Write-Host "$Name is ready"
            return
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "$Name did not become ready within $TimeoutSeconds seconds"
}

if (-not $Force) {
    Write-Host "This deletes Kafka events, MinIO Bronze/Silver data, collector state, and collected source files."
    Write-Host "The X browser profile and Airflow metadata are preserved."
    $confirmation = Read-Host "Type RESET to continue"
    if ($confirmation -cne "RESET") {
        Write-Host "Reset cancelled."
        exit 0
    }
}

Push-Location $projectRoot
try {
    Write-Host "Stopping services that can write pipeline data..."
    Invoke-Compose @("stop", "airflow-scheduler", "spark-worker", "spark-master", "minio", "kafdrop", "schema-registry", "kafka")

    Write-Host "Removing Kafka and MinIO containers to discard pipeline data..."
    Invoke-Compose @("rm", "-f", "minio", "kafdrop", "schema-registry", "kafka")

    $composeConfig = (& docker compose config --format json | ConvertFrom-Json)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed with exit code $LASTEXITCODE"
    }
    $minioVolumeName = $composeConfig.volumes.'minio-data'.name
    & docker volume inspect $minioVolumeName *> $null
    if ($LASTEXITCODE -eq 0) {
        & docker volume rm $minioVolumeName
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to remove MinIO volume $minioVolumeName"
        }
    }

    foreach ($relativePath in $dataTargets) {
        Clear-DirectoryContents -RelativePath $relativePath
    }

    Write-Host "Starting clean infrastructure..."
    Invoke-Compose @("up", "-d", "kafka", "schema-registry", "kafdrop", "minio")
    Wait-HttpEndpoint -Uri "http://localhost:8081/subjects" -Name "Schema Registry"
    Wait-HttpEndpoint -Uri "http://localhost:9000/minio/health/live" -Name "MinIO"

    Invoke-Compose @("run", "--rm", "minio-init")
    Invoke-Compose @(
        "up",
        "-d",
        "--scale",
        "spark-worker=4",
        "spark-master",
        "spark-worker",
        "airflow-scheduler"
    )

    Write-Host ""
    Write-Host "Pipeline data reset completed."
    Write-Host "Preserved: data\x-edge-profile, data\x-cdp-proxy.pid, Docker volume airflow-postgres-data, orchestrator\logs"
}
finally {
    Pop-Location
}
