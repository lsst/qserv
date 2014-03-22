echo "Setting up install environment"

source ${QSERV_SRC_DIR}/eupspkg/env.sh
source ${QSERV_SRC_DIR}/eupspkg/functions.sh

if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
then   
    echo "Setting up eups"
    source "${INSTALL_DIR}/eups/bin/setups.sh"
else
    echo "eups isn't installed in standard location"
fi
