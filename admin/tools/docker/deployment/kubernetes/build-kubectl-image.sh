#!/bin/sh

# Create docker image containing kops tools and scripts

# @author  Fabrice Jammes

set -e
#set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

IMAGE="qserv/kubectl:latest"

echo $DIR

docker build --tag "$IMAGE" "$DIR/kubectl"
docker push "$IMAGE"
