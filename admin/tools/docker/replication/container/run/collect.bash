#!/bin/bash

set -e

HOME="$1"
if [ -z "$HOME" ]; then
    echo "usage: <path>"
    exit 1 
fi
cd $HOME

source /stack/loadLSST.bash
setup -t qserv-dev qserv_distrib
setup -t qserv-dev -r .

build=tmp/replication/container/build
mkdir -p "${build}/lib" && rm -rf "${build}/lib/*"
mkdir -p "${build}/bin" && rm -rf "${build}/bin/*"

bins=$(ls bin/qserv-replica-* bin/qserv-worker-*)
cp ${bins} "${build}/bin/"

libs="$(for l in $(ldd ${bins} | grep '=>' | grep '/' | awk '{print $3}'); do if [[ $l != /lib* && $l != /usr/* ]]; then echo $l; fi; done | sort -u)"
cp ${libs} "${build}/lib/"

cp admin/tools/docker/replication/container/run/Dockerfile "${build}/"
