#!/bin/sh

# Launched by LSST CI at development build

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

# Build container image for Qserv development version
# i.e. tagged "qserv-dev"
"$DIR"/../1_build-deps.sh -C
"$DIR"/../3_build-configured-images.sh
