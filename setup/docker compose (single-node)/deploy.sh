#!/bin/bash

docker plugin install grafana/loki-docker-driver:2.9.1 --alias loki --grant-all-permissions

if [ "$1" != "" ]; then
    docker compose up -d --build --scale daemon=`./local_daemon/load-num-replicas.sh $1`
else
    docker compose up -d --build --scale daemon=`./local_daemon/load-num-replicas.sh`
fi 
