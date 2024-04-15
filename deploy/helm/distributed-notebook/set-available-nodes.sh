#!/bin/sh

# Pass as an argument the number of nodes you'd like to have available initially/currently.
# This will add NoExecute and NoSchedule taints to additional nodes beyond the specified quantity.

i=0

echo "Number of nodes to be made available: $1"

a=2462620
b=2462620

for node in $(kubectl get nodes -o name | grep worker);
do
    echo ""
    if [ "$i" -ge "$1" ]; then 
        i=$((i + 1))
        echo "Adding taints to node $node"
        kubectl taint nodes $node key1=value1:NoSchedule --overwrite
        kubectl taint nodes $node key2=value2:NoExecute --overwrite
    else
        i=$((i + 1))
        echo "Removing taints from node $node"
        kubectl taint nodes $node key1=value1:NoSchedule-
        kubectl taint nodes $node key2=value2:NoExecute-
    fi
done 