# Orbit local CLI (`orbit`) installation script for Windows.
# Supports Windows on x86_64.
#
# Usage (one-liner, in PowerShell):
#   irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
#
# Usage (with options):
#   irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 -OutFile install.ps1
#   .\install.ps1 -Version v0.58.0
#   .\install.ps1 -Force
#   .\install.ps1 -InstallDir "C:\Tools\orbit"

[CmdletBinding()]
param(
    [string]$Version = "",
    [string]$InstallDir = "$env:LOCALAPPDATA\Programs\orbit",
    [switch]$Force,
    [switch]$Help
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BinaryName = "orbit"
$Platform   = "windows"
$Arch       = "x86_64"
$ProjectId  = "77960826"

function Write-Info    { Write-Host $args -ForegroundColor Cyan }
function Write-OK      { Write-Host $args -ForegroundColor Green }
function Write-Warn    { Write-Host $args -ForegroundColor Yellow }
function Write-Err     { Write-Host $args -ForegroundColor Red }

if ($Help) {
@"
Usage: install.ps1 [OPTIONS]

OPTIONS:
    -Version VERSION         Install specific version (e.g., v0.58.0). Defaults to latest.
    -InstallDir INSTALL_DIR  Install to a custom directory (default: $env:LOCALAPPDATA\Programs\orbit).
    -Force                   Reinstall even if orbit already exists.
    -Help                    Show this help message.
"@
    return
}

Write-Info "=== Orbit local CLI installation ==="
Write-Host ""

# Normalize version (allow "0.58.0" -> "v0.58.0").
if ($Version -and $Version -notmatch '^v') { $Version = "v$Version" }
if ($Version -and $Version -notmatch '^v\d+\.\d+\.\d+(-[A-Za-z0-9.]+)?$') {
    throw "Invalid version format: $Version (expected vX.Y.Z or vX.Y.Z-suffix)"
}

$binaryPath = Join-Path $InstallDir "$BinaryName.exe"
if ((Test-Path $binaryPath) -and -not $Force) {
    Write-Warn "Orbit local CLI is already installed at $binaryPath"
    Write-Host "Re-run with -Force to reinstall, or -Version vX.Y.Z to switch versions."
    return
}

if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}

if (-not $Version) {
    Write-Host "Resolving the latest Orbit release..."
    $permalink = "https://gitlab.com/api/v4/projects/$ProjectId/releases/permalink/latest"
    try {
        $release = Invoke-RestMethod -UseBasicParsing -Uri $permalink
    } catch {
        throw "Failed to resolve latest release from $permalink : $_"
    }
    $Version = $release.tag_name
    if (-not $Version) { throw "Latest release returned no tag_name." }
}

Write-Host "Installing the Orbit local CLI $Version..."

$pkgVersion  = $Version.TrimStart('v')
$artifact    = "orbit-local-$Platform-$Arch.zip"
$pkgBase     = "https://gitlab.com/api/v4/projects/$ProjectId/packages/generic/orbit-local/$pkgVersion"
$downloadUrl = "$pkgBase/$artifact"
$checksumUrl = "$pkgBase/$artifact.sha256"

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

try {
    $zipPath      = Join-Path $tempDir $artifact
    $checksumPath = "$zipPath.sha256"

    Write-Host "Downloading $artifact..."
    Invoke-WebRequest -UseBasicParsing -Uri $downloadUrl -OutFile $zipPath

    Write-Host "Downloading checksum..."
    Invoke-WebRequest -UseBasicParsing -Uri $checksumUrl -OutFile $checksumPath

    Write-Host "Verifying checksum..."
    $expected = ((Get-Content $checksumPath -Raw).Trim() -split '\s+')[0].ToLower()
    $actual   = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash.ToLower()
    if ($expected -ne $actual) {
        throw "Checksum mismatch.`nExpected: $expected`nActual:   $actual"
    }
    Write-OK "Checksum verified."

    Write-Host "Extracting..."
    $extractDir = Join-Path $tempDir "extract"
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

    $exe = Get-ChildItem -Path $extractDir -Recurse -Filter "$BinaryName.exe" | Select-Object -First 1
    if (-not $exe) { throw "$BinaryName.exe not found in archive." }

    Write-Host "Installing to $InstallDir..."
    Copy-Item -Path $exe.FullName -Destination $binaryPath -Force
}
finally {
    Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue
}

# Add to User PATH (per-user, no admin required).
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
$paths    = if ($userPath) { $userPath.Split(';') } else { @() }
if ($paths -notcontains $InstallDir) {
    Write-Host "Adding $InstallDir to user PATH..."
    $newPath = if ($userPath) { "$InstallDir;$userPath" } else { $InstallDir }
    [Environment]::SetEnvironmentVariable("PATH", $newPath, "User")
    Write-Warn "PATH updated. Open a new terminal for the change to take effect."
} else {
    Write-Host "$InstallDir is already in user PATH."
}

$env:PATH = "$InstallDir;$env:PATH"

Write-OK "Orbit local CLI installed at $binaryPath"
Write-Host ""
Write-Host "Verify with:  orbit --help"
