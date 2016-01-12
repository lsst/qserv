. ./env.sh

ssh "$MASTER" "docker pull ${MASTER_IMAGE}"
shmux -c "docker pull ${WORKER_IMAGE}" $WORKERS 
