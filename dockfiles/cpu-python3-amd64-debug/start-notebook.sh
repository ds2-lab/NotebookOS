#!/bin/bash
# Shim to emit warning and call start-notebook.py
echo "WARNING: Use start-notebook.py instead"

# exec /usr/local/bin/start-notebook.py "$@"
/home/jovyan/Python-3.11.9/debug/python /usr/local/bin/start-notebook.py "$@"
# gdb -ex run --args /home/jovyan/Python-3.11.9/debug/python /usr/local/bin/start-notebook.py "$@"