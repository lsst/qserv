# Directory containing infrastructure specification
# (ssh credentials, machine names)
CLUSTER_CONFIG_DIR="$HOME/.lsst/qserv-cluster"

# ssh credentials
SSH_CFG="$HOME/.lsst/qserv-cluster/ssh_config"

# Machine names
ENV_INFRASTRUCTURE_FILE="$CLUSTER_CONFIG_DIR/env-infrastructure.sh"
. "$ENV_INFRASTRUCTURE_FILE"
