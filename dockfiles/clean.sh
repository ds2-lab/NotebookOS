#!/bin/bash

# Get a list of all Docker images
images=$(docker images -a --format "{{.Repository}}:{{.ID}}")

# Loop through each image and remove it if it has a tag of "<none>"
while read -r image; do
  repo=$(echo $image | cut -d ":" -f 1)
  if [ "$repo" == "<none>" ]; then
    id=$(echo $image | cut -d ":" -f 2)
    docker rmi $id
  fi
done <<< "$images"

# Get a list of all exited Docker containers
containers=$(docker ps -a --format "{{.ID}}:{{.Status}}" | grep Exited | cut -d ":" -f 1)

# Loop through each container and remove it
while read -r container; do
  if [ -n "$container" ]; then
    docker rm $container
  fi
done <<< "$containers"