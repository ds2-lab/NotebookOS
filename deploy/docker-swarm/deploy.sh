#!/bin/bash

docker stack deploy --compose-file ./docker-compose.yml distributed_cluster --detach=false