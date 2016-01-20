. ./env.sh

ssh "$MASTER" "docker pull ${MASTER_IMAGE}"
shmux -S all -c "docker pull ${WORKER_IMAGE}" $WORKERS
