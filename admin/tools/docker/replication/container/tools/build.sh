#!/bin/bash

set -e

USAGE="usage: <path> {gke|ncsa} [<tag>]"
SOURCE="$1"
if [ -z "$SOURCE" ]; then
    echo $USAGE
    exit 1
fi
DEST="$2"
case $DEST in
  gke | ncsa)
    ;;
  *)
    echo $USAGE
    exit 1
    ;;
esac
TAG="$3"
if [ -z "$TAG" ]; then
    TAG="qserv/replica:tools"
fi

cd $SOURCE

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
       qserv/replica:dev_conda \
       bash -c '$SOURCE/admin/tools/docker/replication/container/tools/collect.sh $SOURCE'

echo "************************************************************************************************"
echo "building ${TAG}"
echo "************************************************************************************************"
docker build \
       -t ${TAG} \
       -f admin/tools/docker/replication/container/tools/Dockerfile.$DEST \
       tmp/replication/container/build

echo "************************************************************************************************"
echo "pushing ${TAG} to DockerHub"
echo "************************************************************************************************"
docker push ${TAG}
