# Wildfire Twin - Unified Pipeline Launcher
# This script launches all necessary components in separate PowerShell windows

$ErrorActionPreference = "Stop"

Write-Host "=== Starting Wildfire Twin Pipeline ===" -ForegroundColor Cyan

# 1. Start Docker (Kafka & Zookeeper)
Write-Host "1. Starting Kafka via Docker Compose..."
cd infra
docker-compose up -d
cd ..

# Wait for Kafka to be ready
Write-Host "Waiting 10 seconds for Kafka to initialize..."
Start-Sleep -Seconds 10

# Helper to launch a new process
function Launch-Component($title, $command) {
    Write-Host "Starting $title..."
    Start-Process powershell -ArgumentList "-NoExit -Command `$Host.UI.RawUI.WindowTitle='$title'; .\wildfire\Scripts\activate; $command"
}

# 2. Start Alert Sink Consumer
Launch-Component "Alert Sink Consumer" "python alert_sink/consumer.py"

# Wait a moment
Start-Sleep -Seconds 2

# 3. Start Spark Spatial Engine
Launch-Component "Spark Spatial Engine" "python spark_processor/spatial_engine.py"

# Wait a moment
Start-Sleep -Seconds 5

# 4. Start Streamlit Dashboard
Launch-Component "Streamlit Dashboard" "streamlit run dashboard/backend/app.py"

Write-Host "=== All services launched! ===" -ForegroundColor Green
Write-Host "Note: Fire simulator is NOT running continuously by default."
Write-Host "To simulate a fire manually, run: python scripts/simulate_fire.py"
