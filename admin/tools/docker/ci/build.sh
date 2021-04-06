#!/bin/sh

set -eux

TAG="$(git describe --dirty --always)"
BASE_IMG_TAG="$TAG"
IMG="qserv/lite-qserv:$TAG"
docker build --build-arg TAG="$BASE_IMG_TAG" --tag="$IMG" .
docker push "$IMG"
