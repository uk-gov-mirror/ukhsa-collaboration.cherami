#!/bin/bash

set -e

if kubectl get pod rabbitmq &> /dev/null; then
    kubectl delete pod rabbitmq
    echo "Stopping rabbitmq pod"
fi

    
