setup protobuf
setup xrootd
setup mysql
export PROTOC=${PROTOBUF_DIR}/bin/protoc
export PROTOC_INC=${PROTOBUF_DIR}/include
export PROTOC_LIB=${PROTOBUF_DIR}/lib
# Needed by common, cf. qserv/common/detect_deps.py, line 86
export MYSQL_ROOT=${MYSQL_DIR}

# Needed by master and worker
export SEARCH_ROOTS="${XROOTD_DIR}:${MYSQL_DIR}"

# see $(grep -r environ *)
export XRD_DIR="${XROOTD_DIR}"
export XRD_PLATFORM=?

# see scons.sh in core : howto retrieve MySQL libs path ?

#export CPPFLAGS="-I${XROOTD_DIR}/include/xrootd -I${MYSQL_DIR}/include"
#export LIBS="-L${XROOTD_DIR}/lib -L${MYSQL_DIR}/lib -lmysql"
