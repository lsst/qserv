#!/bin/sh

# Perform docker image and container cleanup

# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

echo "Perform docker image and container cleanup"
SCRIPT=docker-cleanup.sh
cp "$DIR/remote/$SCRIPT" /tmp/
parallel --onall --slf .. --transfer --keep-order \
    'echo "Running on $(hostname)" && . {} && docker-cleanup && echo "--"' \
    ::: "/tmp/$SCRIPT"

