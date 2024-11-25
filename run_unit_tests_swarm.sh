#!/bin/bash

echo "Running unit tests for Distributed Notebook cluster"
echo "Omitting Local Daemon's DevicePlugin unit tests"

ginkgo run -r --fail-fast --skip-file ./local_daemon/device