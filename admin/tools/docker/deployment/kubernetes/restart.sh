#!/bin/bash

# Create Swarm cluster and launch Qserv integration tests

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
echo "Setup Kubernetes cluster and launch Qserv integration tests"

"$DIR"/delete-qserv-pods.sh
"$DIR"/create-qserv-pods.sh
"$DIR"/run-large-scale-tests.sh
