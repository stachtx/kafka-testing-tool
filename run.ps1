<#
.SYNOPSIS
  One-command runner for Kafka Testing Tool (Windows / PowerShell).

.DESCRIPTION
  Default (local mode): starts Kafka + Schema Registry in Docker (docker-compose.yml), waits until they are ready,
  starts the app with the `local` Spring profile and opens the browser.

  -Remote: skips Docker and connects to an existing cluster; KAFKA_BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL
  must be set (plus any SPRING_KAFKA_* security variables, see README).

.EXAMPLE
  .\run.ps1                    # local Kafka in Docker + app + browser
  .\run.ps1 -Port 9090         # different HTTP port
  .\run.ps1 -Remote            # existing cluster, settings from environment variables
  .\run.ps1 -Remote -Profiles private   # plus application-private.yml
  .\run.ps1 -Stop              # stop the local Kafka/Registry containers and delete their data
#>
[CmdletBinding()]
param(
    [switch]$Remote,
    [switch]$Stop,
    [switch]$NoBrowser,
    [int]$Port = 8080,
    [string]$Profiles = ''
)

# 'Continue': Windows PowerShell 5.1 turns any native stderr line (docker/gradle warnings) into an error under 'Stop'.
# Native exit codes are checked explicitly below.
$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot

function Write-Step($text) { Write-Host "==> $text" -ForegroundColor Cyan }
function Fail($text) { Write-Host "ERROR: $text" -ForegroundColor Red; exit 1 }

function Test-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Fail 'Docker not found. Install Docker Desktop, or use -Remote with an existing cluster.'
    }
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { Fail 'Docker daemon is not running. Start Docker Desktop and retry.' }
}

function Find-Jdk {
    # Gradle only needs a JDK 17+ to start; it then finds the Java 21 toolchain by itself.
    if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\java.exe")) { return $env:JAVA_HOME }
    $onPath = Get-Command java -ErrorAction SilentlyContinue
    if ($onPath) { return (Split-Path (Split-Path $onPath.Source)) }
    $roots = @("$env:USERPROFILE\.jdks", "$env:ProgramFiles\Java", "$env:ProgramFiles\Eclipse Adoptium",
               "$env:ProgramFiles\Microsoft", "$env:ProgramFiles\Amazon Corretto", "$env:ProgramFiles\Zulu")
    $found = foreach ($r in $roots) {
        if (Test-Path $r) { Get-ChildItem $r -Directory | Where-Object { Test-Path "$($_.FullName)\bin\java.exe" } }
    }
    $pick = $found | Where-Object { $_.Name -match '21' } | Select-Object -First 1
    if (-not $pick) { $pick = $found | Sort-Object Name -Descending | Select-Object -First 1 }
    if ($pick) { return $pick.FullName }
    return $null
}

if ($Stop) {
    Test-Docker
    Write-Step 'Stopping local Kafka + Schema Registry (data is deleted)'
    docker compose down -v
    exit $LASTEXITCODE
}

# --- Java ---------------------------------------------------------------------------------------------------
$jdk = Find-Jdk
if (-not $jdk) { Fail 'No JDK found. Install JDK 21 (e.g. Temurin/Corretto) or set JAVA_HOME.' }
$env:JAVA_HOME = $jdk
$env:Path = "$jdk\bin;$env:Path"
Write-Step "Using JDK: $jdk"

# --- Kafka / Schema Registry ----------------------------------------------------------------------------------
$activeProfiles = @()
if ($Remote) {
    if (-not $env:KAFKA_BOOTSTRAP_SERVERS -or -not $env:SCHEMA_REGISTRY_URL) {
        Fail 'Remote mode needs KAFKA_BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL (e.g. $env:KAFKA_BOOTSTRAP_SERVERS = ''broker:9092'').'
    }
    Write-Step "Remote mode: Kafka $($env:KAFKA_BOOTSTRAP_SERVERS), Registry $($env:SCHEMA_REGISTRY_URL)"
} else {
    Test-Docker
    Write-Step 'Starting local Kafka + Schema Registry (docker compose up -d)'
    docker compose up -d
    if ($LASTEXITCODE -ne 0) { Fail 'docker compose up failed.' }

    Write-Step 'Waiting for Schema Registry on http://localhost:8081 (first start pulls images, can take minutes)'
    $ready = $false
    for ($i = 0; $i -lt 90; $i++) {
        try {
            Invoke-WebRequest -Uri 'http://localhost:8081/subjects' -UseBasicParsing -TimeoutSec 3 | Out-Null
            $ready = $true; break
        } catch { Start-Sleep -Seconds 2 }
    }
    if (-not $ready) { Fail 'Schema Registry did not become ready in 3 minutes. Check: docker compose logs' }
    $activeProfiles += 'local'
}
if ($Profiles) { $activeProfiles += $Profiles.Split(',') }

# --- App ----------------------------------------------------------------------------------------------------
$appArgs = "--server.port=$Port"
if ($activeProfiles.Count -gt 0) { $appArgs += " --spring.profiles.active=$($activeProfiles -join ',')" }
$url = "http://127.0.0.1:$Port"

if (-not $NoBrowser) {
    Start-Job -ArgumentList $url -ScriptBlock {
        param($u)
        for ($i = 0; $i -lt 180; $i++) {
            try { Invoke-WebRequest -Uri "$u/api/kafka/health" -UseBasicParsing -TimeoutSec 2 | Out-Null; Start-Process $u; return }
            catch { Start-Sleep -Seconds 2 }
        }
    } | Out-Null
}

Write-Step "Starting app on $url (Ctrl+C to stop)"
try {
    & .\gradlew.bat bootRun "--args=$appArgs" --console=plain
} finally {
    Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
    if (-not $Remote) {
        Write-Host "`nApp stopped. Kafka + Registry containers are still running; stop them with: .\run.ps1 -Stop" -ForegroundColor Yellow
    }
}
