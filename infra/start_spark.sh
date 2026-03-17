#!/usr/bin/env bash
# Wildfire-Twin — Spark Launcher (Linux/Mac)
# Validates JAVA_HOME points to JDK 11, then launches the spatial engine.
set -e

# --- Java Version Check ---
if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME is not set."
    echo "Spark 3.4.x requires Java 11. Set JAVA_HOME, e.g.:"
    echo '  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64'
    exit 1
fi

JAVA_EXE="$JAVA_HOME/bin/java"
if [ ! -x "$JAVA_EXE" ]; then
    echo "ERROR: java not found at $JAVA_EXE"
    echo "Check that JAVA_HOME ($JAVA_HOME) points to a valid JDK installation."
    exit 1
fi

# Parse java version
JAVA_VER=$("$JAVA_EXE" -version 2>&1)
echo "Detected Java:"
echo "$JAVA_VER"

if ! echo "$JAVA_VER" | grep -qE '"(1\.8|11)\.'; then
    echo ""
    echo "WARNING: Spark 3.4.x requires Java 8 or 11."
    echo "Detected version may cause IllegalAccessError at runtime."
    echo "Press Ctrl+C to abort, or wait 5 seconds to continue anyway..."
    sleep 5
fi

# --- Ensure PYSPARK uses the right Python ---
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(which python3)}"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

echo ""
echo "=== Launching Wildfire-Twin Spatial Engine ==="
echo "JAVA_HOME : $JAVA_HOME"
echo "Python    : $PYSPARK_PYTHON"
echo ""

# --- Launch ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python3 "$SCRIPT_DIR/../spark_processor/spatial_engine.py" "$@"
