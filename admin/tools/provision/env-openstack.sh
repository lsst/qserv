# Configuration file for launching tests for Openstack/Docker/Qserv
# deployment procedure

# Choose the configuration file which contains instance parameters
# For NCSA
CONF_FILE="ncsa.conf"

# For Petasky/Galactica
# CONF_FILE="galactica.conf"

# Check if cloud connection parameters are available
if [ -z "$OS_USERNAME" ]; then
    echo "ERROR: Openstack resource file not sourced"
        exit 1    
    fi

# Choose a number of instances to boot
NB_SERVERS=3

