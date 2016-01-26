#!/bin/sh

# Download Docker container related to Qserv integration tests

# @author  Fabrice Jammes, IN2P3/SLAC

. ./env.sh

docker pull "${MASTER_IMAGE}"
docker pull "${WORKER_IMAGE}"
