# Parameters related to infrastructure:
# - node hostnames
. "${DIR}/env-infrastructure.sh"

# Container images names
MASTER_IMAGE="qserv/qserv:${VERSION}_master"
WORKER_IMAGE="qserv/qserv:${VERSION}_worker"

# Pods names
# ==========

MASTER_POD='master'
WORKER_POD_FORMAT='worker-%g'

# List of worker pods (and containers) names
j=1
WORKER_PODS=''
for host in $WORKERS;
do
    WORKER_PODS="$WORKER_PODS worker-$j"
    j=$((j+1));
done
