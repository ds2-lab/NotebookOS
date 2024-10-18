#!/bin/bash

docker plugin install grafana/loki-docker-driver:2.9.1 --alias loki --grant-all-permissions

docker stack deploy --compose-file ./docker-compose.yml distributed_cluster --detach=false