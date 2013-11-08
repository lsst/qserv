#!/bin/bash

QSERV_BASE_DIR=%(QSERV_BASE_DIR)s

rm -rf "${QSERV_BASE_DIR}/qserv/master/dist"

export XRD_DIR="${QSERV_BASE_DIR}/xrootd";
export XRD_PLATFORM="x86_64_linux_26_dbg";
export PROTOC="${QSERV_BASE_DIR}/bin/protoc";
export PROTOC_INC="${QSERV_BASE_DIR}/include";
export PROTOC_LIB="${QSERV_BASE_DIR}/lib";
export MYSQL_ROOT="${QSERV_BASE_DIR}";
export SEARCH_ROOTS="${QSERV_BASE_DIR}";

echo -e "\n"
echo "Building Qserv commons libs"
echo "---------------------------"
cd "${QSERV_BASE_DIR}/qserv/common"
scons
scons master

DEST="${QSERV_BASE_DIR}/qserv/master/python/lsst/qserv/master/"
if [ ! -f ${DEST}/geometry.py ]
then
    echo "Downloading geometry.py" 
    wget -P ${DEST} http://dev.lsstcorp.org/cgit/LSST/DMS/geom.git/plain/python/lsst/geom/geometry.py
fi

echo -e "\n"
echo "Building Qserv master (rpc server)"
echo "----------------------------------"
cd "${QSERV_BASE_DIR}/qserv/master/"
scons
scons install

echo -e "\n"
echo "Building Qserv worker (xrootd plugin)"
echo "-------------------------------------"
cd "${QSERV_BASE_DIR}/qserv/worker/"
scons
