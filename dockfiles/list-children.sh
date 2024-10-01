#!/bin/bash

echo "Listing children of Docker image $1"

for i in $(docker images -q)
do
    docker history $i | grep -q $1 && echo $i
done | sort -u
