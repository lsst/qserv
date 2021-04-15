#!/bin/bash

set -e

USAGE="usage: <path> <tag> [--local-user]"
SOURCE="$1"
if [ -z "$SOURCE" ]; then
    echo $USAGE
    exit 1
fi
TAG="$2"
if [ -z "$TAG" ]; then
    echo $USAGE
    exit 1
fi
PASSWD=$HOME/passwd
GROUP=$HOME/group
LOCAL_USER_FLAG="$3"
if [ ! -z "$LOCAL_USER_FLAG" ]; then
  if [ $LOCAL_USER_FLAG == "--local-user" ]; then
    PASSWD=/etc/passwd
    GROUP=/etc/group
  else
    echo $USAGE
    exit 1
  fi
fi
if [ ! -f $PASSWD ]; then
    echo "File $PASSWD not found"
    exit 1
fi
if [ ! -f $GROUP ]; then
    echo "File $GROUP not found"
    exit 1
fi
cd $SOURCE

# To get the base container defined in envar DEPS_TAG_DEFAULT
source admin/tools/docker/conf.sh
rm -f Dockerfile
cat admin/tools/docker/replication/container/tools/Dockerfile.tmpl | \
  sed 's/{DEPS_TAG_DEFAULT}/'$DEPS_TAG_DEFAULT'/' > Dockerfile

echo "************************************************************************************************"
echo "collecting binaries and library dependencies of ${TAG}"
docker run \
       --rm \
       -u "$(id -u):$(id -g)" \
       -e "SOURCE=${PWD}" \
       -v $PASSWD:/etc/passwd:ro \
       -v $GROUP:/etc/group:ro \
       -v $HOME:$HOME \
       -v $PWD:$PWD \
       qserv/qserv:$DEPS_TAG_DEFAULT \
       bash -c '$SOURCE/admin/tools/docker/replication/container/tools/collect.sh $SOURCE'

echo "************************************************************************************************"
echo "building ${TAG}"
echo "************************************************************************************************"
docker build \
       -t ${TAG} \
       -f Dockerfile \
       tmp/replication/container/build

echo "************************************************************************************************"
echo "pushing ${TAG} to DockerHub"
echo "************************************************************************************************"
docker push ${TAG}
