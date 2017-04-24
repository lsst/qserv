#!/bin/sh

# Launched by LSST CI at each release build 

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

# Build container image for Qserv latest release
"$DIR"/../1_build-image.sh -C
"$DIR"/../4_build-configured-images.sh -i qserv/qserv:latest 
