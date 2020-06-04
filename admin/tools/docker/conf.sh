# Set Docker registry, default to Docker Hub
DOCKER_REPO=${DOCKER_REPO:-qserv/qserv}
DEPS_TAG_PREFIX="deps"

# Default version of Qserv dependencies image
# Useful for travis-ci and local build
DEPS_TAG_DEFAULT="deps_20200604_0327"

# This file contains tag of latest locally built image
# useful for lsst-dm-ci
DEPS_TAG_FILE="/tmp/qserv_deps_tag"

# Improve cache support for docker build
# export DOCKER_BUILDKIT=1
