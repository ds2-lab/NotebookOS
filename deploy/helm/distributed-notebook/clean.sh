#!/bin/bash

echo "Deleting services."
kubectl delete svc $(kubectl get svc | grep -i "kernel" | awk '{print $1}')
echo "Deleting StatefulSets."
kubectl delete statefulset $(kubectl get statefulset | grep -i "kernel" | awk '{print $1}')
echo "Deleting CloneSets."
kubectl delete cloneset $(kubectl get cloneset | grep -i "kernel" | awk '{print $1}')
echo "Deleting ConfigMaps."
kubectl delete configmap $(kubectl get configmap | grep -i "kernel" | awk '{print $1}')
echo "Deleting PVCs."
kubectl delete pvc $(kubectl get pvc | grep -i "node-local-kernel" | awk '{print $1}')
