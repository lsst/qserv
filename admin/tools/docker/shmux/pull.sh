. ./env.sh

shmux -c "docker pull ${MASTER_IMAGE}" $MASTER 
shmux -c "docker pull ${WORKER_IMAGE}" $WORKERS 
