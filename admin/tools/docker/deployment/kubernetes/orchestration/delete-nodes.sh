#!/bin/sh

# Destroy Kubernetes nodes 

# @author Fabrice Jammes SLAC/IN2P3

NODES=$(kubectl get nodes -o go-template --template \
    '{{range .items}}{{.metadata.name}} {{end}}')

for node in $NODES
do
    kubectl drain "$node" --delete-local-data --force --ignore-daemonsets
    kubectl delete node "$node"
done
