#!/bin/bash

# Create Swarm cluster and launch Qserv integration tests

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
echo "Setup Swarm cluster and launch Qserv integration tests"

"$DIR"/swarm-destroy.sh
"$DIR"/swarm-create.sh
"$DIR"/run.sh
"$DIR"/run-multinode-tests.sh
