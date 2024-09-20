#!/bin/bash

echo Building Gateway via Docker
make docker

echo Pushing latest build to Docker Hub
docker push scusemua/gateway:latest

echo Done
