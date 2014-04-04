if [ -n "${QSERV_SRC_DIR}" ]
then
  if [ -r ${QSERV_SRC_DIR} ]
  then
    source ${QSERV_SRC_DIR}/eupspkg/setup.sh
    echo "Setting up distribution environment"
    source ${QSERV_SRC_DIR}/eupspkg/dist/env-dist.sh
  else
    echo "QSERV_SRC_DIR=${QSERV_SRC_DIR} is not set is not readable"
  fi
else
  echo "QSERV_SRC_DIR is not set"
  SETUP_DIST=FAIL
fi

