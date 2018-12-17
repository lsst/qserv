#!/bin/bash

set -e

SOURCE="$1"
if [ -z "$SOURCE" ]; then
    echo "usage: <path>"
    exit 1
fi

cd $SOURCE

GIT_HASH="$(git describe --dirty --always)"
TAG="qserv/replica:tools-${GIT_HASH}"

echo "container tag: ${TAG}"
echo "collecting binaries and their library dependencies"
docker run \
       --rm \
       -u "$(id -u):$(id -g)" \
       -e "SOURCE=${PWD}" \
       -v /etc/passwd:/etc/passwd:ro \
       -v /etc/group:/etc/group:ro \
       -v $PWD:$PWD \
       qserv/replica:dev \
       bash -c '$SOURCE/admin/tools/docker/replication/container/tools/gke/collect.bash $SOURCE'
echo "building the container"
docker build -t ${TAG} tmp/replication/container/build

echo "pushing ${TAG} to DockerHub"
docker push ${TAG}
