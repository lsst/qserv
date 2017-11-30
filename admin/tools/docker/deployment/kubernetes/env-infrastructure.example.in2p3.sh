# Parameters related to Qserv instructure
# Location: ~/.lsst/qserv-cluster
# This example is inspired from IN2P3 Qserv cluster

# All host have same prefix
HOSTNAME_TPL="ccqserv"

# First and last id for worker node names
WORKER_FIRST_ID=101
WORKER_LAST_ID=124

# Used for ssh access
MASTER="${HOSTNAME_TPL}100"

# Used for ssh access
WORKERS=$(seq --format "${HOSTNAME_TPL}%g" --separator=' ' "$WORKER_FIRST_ID" "$WORKER_LAST_ID")

# Used for ssh access to Kubernetes master (i.e. orchestrator)
# Here Kubernetes master and Qserv master pods are on the same machine
ORCHESTRATOR="ccqservkm1"
