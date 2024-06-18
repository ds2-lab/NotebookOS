#!/bin/bash

docker compose up -d --build --scale daemon=`./local_daemon/load-num-replicas.sh`