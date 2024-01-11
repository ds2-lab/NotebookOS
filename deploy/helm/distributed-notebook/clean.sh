#!/bin/bash

echo "Deleting services."
kubectl delete svc $(sudo kubectl get svc | grep nginx-session | awk '{print $1}')
echo "Deleting StatefulSets."
kubectl delete statefulset $(sudo kubectl get statefulset | grep kernel | awk '{print $1}')
echo "Deleting ConfigMaps."
kubectl delete configmap $(sudo kubectl get configmap | grep kernel | awk '{print $1}')
echo "Deleting PVCs."
kubectl delete pvc $(sudo kubectl get pvc | grep node-local-kernel | awk '{print $1}')