#!/bin/bash

# Change this to be whatever the root directory of the Github repo is.
# TODO: Make this a command-line argument.
ROOT_DIR=/home/bcarver2/go/pkg/distributed-notebook/

cd $ROOT_DIR/dockfiles/gateway && make all &
cd $ROOT_DIR/dockfiles/local_daemon && make all &
cd $ROOT_DIR/dockfiles/gateway && make all &
wait 

docker compose up -d --build 