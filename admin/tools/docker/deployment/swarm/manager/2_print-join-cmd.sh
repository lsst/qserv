#!/bin/sh

# Print command to join a worker node to swarm manager
# Current script must be lunach on mananger and pinrted command
# must be launched on worker

# @author  Fabrice Jammes, IN2P3

set -e

# Remove help message at first line
JOIN_CMD=$(docker swarm join-token worker| tail -n +2)

echo "$JOIN_CMD"

