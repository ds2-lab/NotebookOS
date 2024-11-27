#!/bin/bash

echo "Building Local Daemon via Docker"
make docker || { echo 'Failed to build Local Daemon...' ; exit 1 ; }

echo "Finished building Local Daemon via Docker"
echo "Pushing scuemua/daemon:latest to Docker Hub now..."
docker push scusemua/daemon:latest

echo "Done"
