# Wildfire-Twin — Spark Launcher (Windows PowerShell)
# Validates JAVA_HOME points to JDK 11, then launches the spatial engine.

$ErrorActionPreference = "Stop"

# --- Environment Setup (Automatic) ---
$possibleJdkPath = "C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
if (Test-Path $possibleJdkPath) {
    $env:JAVA_HOME = $possibleJdkPath
}

if (-not $env:JAVA_HOME) {
    Write-Host "ERROR: JAVA_HOME is not set." -ForegroundColor Red
    Write-Host "Please set JAVA_HOME to your JDK 11 installation path."
    exit 1
}

# Set Hadoop Home for Windows (Local)
$env:HADOOP_HOME = Join-Path $PSScriptRoot "hadoop"
$env:PATH = "$($env:HADOOP_HOME)\bin;$($env:PATH)"

$javaExe = Join-Path $env:JAVA_HOME "bin\java.exe"
if (-not (Test-Path $javaExe)) {
    Write-Host "ERROR: java.exe not found at $javaExe" -ForegroundColor Red
    Write-Host "Check that JAVA_HOME ($env:JAVA_HOME) points to a valid JDK installation."
    exit 1
}

# Parse java version safely
$javaVersionOutput = ""
try {
    # Java outputs version info to stderr
    $javaVersionOutput = & $javaExe -version 2>&1 | Out-String
} catch {
    # If it fails, try without redirection
    $javaVersionOutput = & $javaExe -version | Out-String
}
Write-Host "Detected Java:" -ForegroundColor Cyan
Write-Host $javaVersionOutput

if ($javaVersionOutput -notmatch '"(1\.8|11)\.' ) {
    Write-Host "WARNING: Spark 3.4.x requires Java 8 or 11." -ForegroundColor Yellow
    Write-Host "Detected version may cause IllegalAccessError at runtime."
    Write-Host "Press Ctrl+C to abort, or wait 5 seconds to continue anyway..."
    Start-Sleep -Seconds 5
}

# --- Python Setup ---
$venvPython = Join-Path $PSScriptRoot "..\wildfire\Scripts\python.exe"
if (Test-Path $venvPython) {
    $env:PYTHON_EXECUTABLE = $venvPython
} else {
    $env:PYTHON_EXECUTABLE = (Get-Command python).Source
}
$env:PYSPARK_PYTHON = $env:PYTHON_EXECUTABLE
$env:PYSPARK_DRIVER_PYTHON = $env:PYTHON_EXECUTABLE

Write-Host "Detected Python : $env:PYTHON_EXECUTABLE" -ForegroundColor Cyan

Write-Host ""
Write-Host "=== Launching Wildfire-Twin Spatial Engine ===" -ForegroundColor Green
Write-Host "JAVA_HOME : $env:JAVA_HOME"
Write-Host "Python    : $env:PYSPARK_PYTHON"
Write-Host ""

# --- Launch ---
python "$PSScriptRoot\..\spark_processor\spatial_engine.py" @args
