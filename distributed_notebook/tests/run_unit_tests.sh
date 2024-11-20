#!/bin/bash

echo "Running unit tests with SIMULATE_CHECKPOINTING_LATENCY equal to 'true'"
SIMULATE_CHECKPOINTING_LATENCY=true pytest --log-cli-level=DEBUG

sleep 2

# Store the exit code.
exit_code=$?

# Per the py_test documentation, it returns 0 when all tests pass.
# So, we'll only try running the next batch of unit tests if the return code was 0.
if [ $exit_code -eq 0 ]; then
  echo "Running unit tests with SIMULATE_CHECKPOINTING_LATENCY unset"
  pytest --log-cli-level=DEBUG
else
  echo "Some unit tests did not pass."
fi

echo "Done."