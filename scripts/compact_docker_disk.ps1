$ErrorActionPreference = "Stop"

$vhdPath = Join-Path $env:LOCALAPPDATA "Docker\wsl\disk\docker_data.vhdx"
$dockerRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $env:LOCALAPPDATA "Docker\wsl\disk")
)
$resolvedVhd = [System.IO.Path]::GetFullPath($vhdPath)

if (-not $resolvedVhd.StartsWith(
    $dockerRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "Unexpected Docker VHD path: $resolvedVhd"
}
if (-not (Test-Path -LiteralPath $resolvedVhd)) {
    throw "Docker VHD not found: $resolvedVhd"
}
if (Get-Process "Docker Desktop", "com.docker.backend" -ErrorAction SilentlyContinue) {
    throw "Close Docker Desktop before compacting its disk."
}

wsl --shutdown
Start-Sleep -Seconds 3

$before = (Get-Item -LiteralPath $resolvedVhd).Length
$diskpartScript = Join-Path $env:TEMP "compact-docker-vhd.txt"
try {
    @(
        "select vdisk file=`"$resolvedVhd`""
        "compact vdisk"
        "exit"
    ) | Set-Content -LiteralPath $diskpartScript -Encoding ASCII

    & diskpart /s $diskpartScript
    if ($LASTEXITCODE -ne 0) {
        throw "DiskPart failed with exit code $LASTEXITCODE"
    }
}
finally {
    Remove-Item -LiteralPath $diskpartScript -Force -ErrorAction SilentlyContinue
}

$after = (Get-Item -LiteralPath $resolvedVhd).Length
$reclaimed = $before - $after
Write-Host (
    "Docker disk compacted: {0:N2} GB -> {1:N2} GB ({2:N2} GB reclaimed)" -f
    ($before / 1GB),
    ($after / 1GB),
    ($reclaimed / 1GB)
)
