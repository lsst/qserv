#!/bin/sh

# Launched by LSST CI at development build

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

# Build container iamge for Qserv development version
"$DIR"/../1_build-image.sh -CD
"$DIR"/../4_build-configured-images.sh
