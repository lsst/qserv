#!/bin/bash

# Install eups packages from source
# Use SRC_DIR to specify a list of source directory
# to build in a given order

set -e
set -x

if [ -z "$SRC_DIRS" ]; then
    echo "ERROR: undefined \$SRC_DIRS"
    exit 2
fi

# Launch eups install
. /qserv/stack/loadLSST.bash
for SRC_DIR in $SRC_DIRS
do
{ 
# Use subshell to get out SRC_DIR if build crash
echo "Install from source: $SRC_DIR"
cd "$SRC_DIR"
setup -r . -t qserv-dev
eupspkg -er config
eupspkg -er install
eupspkg -er decl -t qserv-dev
}
done

# Set path to eups product in /etc/sysconfig/qserv
# TODO make it simpler
QSERV_RUN_DIR=/qserv/run

setup qserv_distrib -t qserv-dev
qserv-configure.py --init --force --qserv-run-dir "$QSERV_RUN_DIR"
qserv-configure.py --etc --qserv-run-dir "$QSERV_RUN_DIR" --force
rm "$QSERV_RUN_DIR"/qserv-meta.conf
