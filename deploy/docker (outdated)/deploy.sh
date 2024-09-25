#!/bin/bash

if [ "$1" != "" ]; then
    docker compose up -d --build --scale daemon=`./local_daemon/load-num-replicas.sh $1`
else
    docker compose up -d --build --scale daemon=`./local_daemon/load-num-replicas.sh`
fi 
