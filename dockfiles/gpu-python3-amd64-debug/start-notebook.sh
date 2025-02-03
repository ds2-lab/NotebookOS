#!/bin/bash
# Shim to emit warning and call start-notebook.py
echo "WARNING: Use start-notebook.py instead"

echo "start-notebook.sh script has been executed."

exec /usr/local/bin/start-notebook.py "$@"

echo "Application exited with code $?"

sleep 2

echo "Exiting now"