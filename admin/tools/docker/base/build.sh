i#!/bin/sh

set -eux

TAG="$(git describe --dirty --always)"

docker build  --target=lite-build --tag=qserv/lite-build:$TAG .
docker build  --tag=qserv/lite-run-base:$TAG .
docker push qserv/lite-build:$TAG
docker push qserv/lite-run-base:$TAG
