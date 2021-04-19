#!/bin/bash

set -euxo pipefail

DIR="$(cd "$(dirname "$0")"; pwd -P)"

TAG="$(git -C "$DIR" describe --dirty --always)"

docker build  --target=lite-build --tag=qserv/lite-build:$TAG .
docker build  --tag=qserv/lite-run-base:$TAG .