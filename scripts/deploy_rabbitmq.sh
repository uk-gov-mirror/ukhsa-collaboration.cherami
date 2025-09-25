#!/bin/bash

set -e

cat <<EOF > rabbitmq_pod.yaml
apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: null
  labels:
    run: rabbitmq
  name: rabbitmq
spec:
  containers:
  - image: rabbitmq:management
    name: rabbitmq
    ports:
    - containerPort: 5672
    resources:
      requests:
          cpu: "1000m"
          memory: "512Mi"
  dnsPolicy: ClusterFirst
  restartPolicy: Always
status: {}
EOF

echo "--Deploying rabbitmq--"
# try deploy pod
kubectl apply -f rabbitmq_pod.yaml
kubectl wait --for=condition=Ready pod/rabbitmq --timeout=300s

echo "Deployed:"
# show status if deployed
kubectl get pod rabbitmq

echo "--Pod IP Address--"
POD_IP=$(kubectl get pod rabbitmq -o jsonpath='{.status.podIP}')
# k8 DNS with pod names doesnt seem to work - pritning IP for easy copy/paste
echo "RabbitMQ Pod IP: $POD_IP"
echo "export RABBITMQ_IP=$POD_IP"

rm rabbitmq_pod.yaml
echo "To stop run: kubectl delete pod rabbitmq"