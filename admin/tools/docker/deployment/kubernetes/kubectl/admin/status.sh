#!/bin/bash

# Manage Qserv services inside pods 

# @author Fabrice Jammes SLAC/IN2P3

set -e

# Return service status as default behaviour

echo "PODS STATUS:"
kubectl get pods -o wide

echo
echo "CONTAINER PER PODS:"
kubectl get pods \
    -o=jsonpath='{range .items[*]}{"\n"}{.metadata.name}{":\t"}{range .spec.containers[*]}{.image}{", "}{end}{end}' |\
    sort
