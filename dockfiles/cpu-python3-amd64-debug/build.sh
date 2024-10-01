#!/bin/bash

echo "Building Jupyter debug image now..."

make build

echo "Finished building Jupyter debug image"
echo "Pushing image to Docker Hub now..."

docker push scusemua/jupyter-debug:latest

echo "Done"
