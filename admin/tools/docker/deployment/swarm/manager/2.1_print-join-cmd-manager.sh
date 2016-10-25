#!/bin/sh

# Print command to join a manager node to swarm cluster
# Current script must be lunach on manager and printed command
# must be launched on other manager

# @author  Fabrice Jammes, IN2P3

set -e

# Remove help message at first line
JOIN_CMD=$(docker swarm join-token manager| tail -n +2)

echo "$JOIN_CMD"

