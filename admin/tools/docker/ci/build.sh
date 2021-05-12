#!/bin/bash

# Build lite-qserv container image

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR="$(cd "$(dirname "$0")"; pwd -P)"

TAG="$(git -C "$DIR" describe --dirty --always)"

usage() {
  cat << EOD

Usage: `basename $0`[base_image_tag]

  Available options:
    -h          this message

  Build lite-qserv container image
  If defined use <base_image_tag> for base images (qserv/lite-build and qserv/lite-run-base)
  else use $TAG
EOD
}

if [ $# -gt 1 ] ; then
    usage
    exit 2
fi

BASE_IMG_TAG="${1:-$TAG}"

IMG="qserv/lite-qserv:$TAG"
docker build --build-arg TAG="$BASE_IMG_TAG" --tag="$IMG" "$DIR"
