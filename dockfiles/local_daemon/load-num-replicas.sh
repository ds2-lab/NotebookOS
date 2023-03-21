#!/bin/bash

PLUS=0
if [ "$1" != "" ]; then
    PLUS=$1
fi

NUM=`grep num-replicas ../gateway/gateway.yml | cut -d ':' -f 2 | sed 's/^[ \t]*//'`

echo $(($NUM+$PLUS))