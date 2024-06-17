#!/bin/bash

# Start all of the components in the Docker-based distributed notebook cluster.

if [ ! -d /home/$USER/kernel_base ]; then
  mkdir -p /home/$USER/kernel_base;
fi

export COMPOSE_FILES="$(find . | grep "docker-compose.yml")"

for COMPOSE_FILE in $COMPOSE_FILES
do
    docker-compose -f $COMPOSE_FILE --env-file ./.env up -d --build
done