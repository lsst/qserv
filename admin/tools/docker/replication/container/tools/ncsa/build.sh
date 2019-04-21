#!/bin/bash

set -e

SOURCE="$1"
if [ -z "$SOURCE" ]; then
    echo "usage: <path>"
    exit 1
fi

cd $SOURCE

TAG="qserv/replica:tools"

echo "************************************************************************************************"
echo "collecting binaries and library dependencies of ${TAG}"
docker run \
       --rm \
       -u "$(id -u):$(id -g)" \
       -e "SOURCE=${PWD}" \
       -v /etc/passwd:/etc/passwd:ro \
       -v /etc/group:/etc/group:ro \
       -v $HOME:$HOME \
       -v $PWD:$PWD \
       qserv/replica:dev \
       bash -c '$SOURCE/admin/tools/docker/replication/container/tools/ncsa/collect.sh $SOURCE'

echo "************************************************************************************************"
echo "building ${TAG}"
echo "************************************************************************************************"
docker build \
       -t ${TAG} \
       -f admin/tools/docker/replication/container/tools/ncsa/Dockerfile \
       tmp/replication/container/build

echo "************************************************************************************************"
echo "pushing ${TAG} to DockerHub"
echo "************************************************************************************************"
docker push ${TAG}
