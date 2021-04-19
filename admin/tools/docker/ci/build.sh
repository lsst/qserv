#!/bin/bash

set -euxo pipefail

DIR="$(cd "$(dirname "$0")"; pwd -P)"

TAG="$(git -C "$DIR" describe --dirty --always)"
BASE_IMG_TAG="$TAG"
IMG="qserv/lite-qserv:$TAG"
docker build --build-arg TAG="$BASE_IMG_TAG" --tag="$IMG" .
