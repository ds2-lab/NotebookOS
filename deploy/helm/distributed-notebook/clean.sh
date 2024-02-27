#!/bin/bash

echo "Deleting services."
kubectl delete svc $(kubectl get svc | grep kernel | awk '{print $1}')
echo "Deleting StatefulSets."
kubectl delete statefulset $(kubectl get statefulset | grep kernel | awk '{print $1}')
echo "Deleting CloneSets."
kubectl delete cloneset $(kubectl get cloneset | grep kernel | awk '{print $1}')
echo "Deleting ConfigMaps."
kubectl delete configmap $(kubectl get configmap | grep kernel | awk '{print $1}')
echo "Deleting PVCs."
kubectl delete pvc $(kubectl get pvc | grep node-local-kernel | awk '{print $1}')
