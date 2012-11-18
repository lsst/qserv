#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

mkdir -p ${QSERV_BASE}/build
cd ${QSERV_BASE}/build

wget -e robots=off -nH --cut-dirs=5  -r -l 1 -np --reject="index.html*" http://www.slac.stanford.edu/exp/lsst/qserv/download/current

