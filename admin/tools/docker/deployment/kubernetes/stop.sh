#!/bin/bash

# Stop Qserv pods and wait for them to be removed

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

echo "Delete Qserv pods on Kubernetes cluster"

"$DIR"/run-kubectl.sh -C "/root/admin/stop.sh"
