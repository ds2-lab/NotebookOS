#!/bin/bash

# Set STACK_NAME to the first command line argument if it exists and is not empty
# Otherwise, default to "distributed_notebook"
STACK_NAME=${1:-distributed_notebook}

echo "(Re)deploying docker stack '$STACK_NAME'"

# Deploy the Docker stack with the provided or default STACK_NAME
docker stack deploy -c docker-compose.yml "$STACK_NAME"