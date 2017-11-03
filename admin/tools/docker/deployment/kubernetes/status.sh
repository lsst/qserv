#!/bin/bash

# Get status for Qserv pods and services 

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

echo
echo "Check that Qserv master and workers pods are running on all nodes"
echo "================================================================="
echo
"$DIR"/run-kubectl.sh -C "kubectl get pods -l app=qserv"

echo
echo "Check that Qserv services are running on all these pods"
echo "======================================================="
echo
"$DIR"/run-kubectl.sh -C "/root/admin/status.sh"
