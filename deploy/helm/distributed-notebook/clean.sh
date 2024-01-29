#!/bin/bash

echo "Deleting services."
sudo kubectl delete svc $(sudo kubectl get svc | grep kernel | awk '{print $1}')
echo "Deleting StatefulSets."
sudo kubectl delete statefulset $(sudo kubectl get statefulset | grep kernel | awk '{print $1}')
echo "Deleting CloneSets."
sudo kubectl delete cloneset $(sudo kubectl get cloneset | grep kernel | awk '{print $1}')
echo "Deleting ConfigMaps."
sudo kubectl delete configmap $(sudo kubectl get configmap | grep kernel | awk '{print $1}')
echo "Deleting PVCs."
sudo kubectl delete pvc $(sudo kubectl get pvc | grep node-local-kernel | awk '{print $1}')
