#!/bin/bash

# Copy orchestration script to kubernetes master
# Start Qserv pods
# Wait for Qserv startup

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

"$DIR"/run-kubectl.sh -C "/root/admin/start.sh"
"$DIR"/run-kubectl.sh -C "/root/admin/wait-pods-start.sh"
"$DIR"/run-kubectl.sh -C "/root/admin/wait-qserv-start.sh"
