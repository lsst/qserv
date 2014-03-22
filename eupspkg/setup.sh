echo "Setting up install environment"

source env.sh
source functions.sh

if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
then   
    echo "Setting up eups"
    source "${INSTALL_DIR}/eups/bin/setups.sh"
else
    echo "eups isn't installed in standard location"
fi
