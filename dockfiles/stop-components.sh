#!/bin/bash

# Stop all of the components in the Docker-based distributed notebook cluster.

export COMPOSE_FILES="$(find . | grep "docker-compose.yml")"

for COMPOSE_FILE in $COMPOSE_FILES
do
    docker-compose -f $COMPOSE_FILE --env-file ./.env down -d --build
done