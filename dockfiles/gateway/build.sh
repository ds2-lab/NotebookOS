#!/bin/bash

echo Building Gateway via Docker
make docker || { echo 'Failed to build the Cluster Gateway...' ; exit 1; }

echo Pushing latest build to Docker Hub
docker push scusemua/gateway:latest

echo Done
