#!/bin/bash

# This won't work unless you call it from the "deploy/docker/" directory.
# The path to the `gateway.yml` is relative to that location.
# In particular, this script is meant to be called by the "deploy/docker/deploy.sh" script.

PLUS=0
if [ "$1" != "" ]; then
    PLUS=$1
fi

NUM=`grep num-replicas ./gateway/gateway.yml | cut -d ':' -f 2 | sed 's/^[ \t]*//'`

echo $(($NUM+$PLUS))