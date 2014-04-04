echo "Setting up install environment"

if [ -n "${QSERV_SRC_DIR}" ]
then
  if [ -r ${QSERV_SRC_DIR} ]
  then
    source ${QSERV_SRC_DIR}/eupspkg/env.sh
    source ${QSERV_SRC_DIR}/eupspkg/functions.sh

    if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
    then
        echo "Setting up eups"
        source "${INSTALL_DIR}/eups/bin/setups.sh"
    else
        echo "eups isn't installed in standard location"
    fi
  else
    echo "QSERV_SRC_DIR=${QSERV_SRC_DIR} is not set is not readable"
  fi
else
  echo "QSERV_SRC_DIR is not set"
  SETUP=FAIL
fi

