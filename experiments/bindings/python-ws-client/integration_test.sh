#!/bin/bash

# --- Script Configuration ---
# Exit script on any error
set -e

# Relative path to the root 'experiments' directory from this script's location
EXPERIMENTS_DIR_REL_PATH="../../"

# Server and test settings
SERVER_PACKAGE_NAME="simd-r-drive-ws-server"
STORAGE_FILE="/tmp/simd-r-drive-pytest-storage.bin"
SERVER_ADDR="127.0.0.1:34129"
SERVER_PID=""

# --- Cleanup Function ---
# This function is called on script exit to ensure resources are released.
cleanup() {
    echo "--- Initiating Cleanup ---"
    if [[ ! -z "$SERVER_PID" ]]; then
        echo "--> Shutting down server (PID: $SERVER_PID)..."
        # Kill the entire process group to stop the server and any children.
        # The '|| true' prevents the script from failing if the process is already gone.
        kill -9 "-$SERVER_PID" 2>/dev/null || true
    fi
    echo "--> Removing temporary storage file: $STORAGE_FILE..."
    rm -f "$STORAGE_FILE"
    echo "--- Cleanup Complete ---"
}

# Register the cleanup function to be called on script exit
trap cleanup EXIT

# --- Main Execution ---
echo "--- Starting Integration Test ---"

# Navigate to the 'experiments' directory to build the server
cd "$(dirname "$0")/$EXPERIMENTS_DIR_REL_PATH"
EXPERIMENTS_DIR_ABS=$(pwd)
echo "Changed directory to: $EXPERIMENTS_DIR_ABS"

echo "--> Building the WebSocket server (if needed)..."
# Using 'cargo run' will build the package if it's not up-to-date.
# No need for a separate 'cargo build' step.

echo "--> Starting server in background mode..."
# 'set -m' enables job control, allowing us to run the server in the background
# and capture its PID correctly.
set -m
# Use 'cargo run' to start the server, which is more reliable than a direct path.
# The '--' separates cargo's arguments from the application's arguments.
cargo run --package "$SERVER_PACKAGE_NAME" -- "$STORAGE_FILE" --listen "$SERVER_ADDR" &
SERVER_PID=$!
set +m
echo "Server process started with PID: $SERVER_PID."

# Navigate back to the Python client directory to run tests
cd "$EXPERIMENTS_DIR_ABS/bindings/python-ws-client"
echo "Changed directory to: $(pwd)"

# Check if uv is installed
if ! command -v uv &> /dev/null
then
    echo "--> 'uv' command not found. Please install uv to continue."
    echo "    See: https://github.com/astral-sh/uv"
    exit 1
fi

echo "--> Setting up Python environment with uv..."
# Create a virtual environment using uv.
uv venv

echo "--> Installing Python dependencies using uv..."
uv pip install --quiet pytest maturin
echo "--> Installing development dependencies..."
uv pip install -e . --group dev

pwd

echo "--> Running pytest..."
# Export the server address so the Python test script can use it
export TEST_SERVER_ADDR=$SERVER_ADDR
# Run pytest using the virtual environment's executable
uv run pytest -v -s

echo "--- Test Completed Successfully ---"
# The 'trap' will handle cleanup automatically upon script exit
