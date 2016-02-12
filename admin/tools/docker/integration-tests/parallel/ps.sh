. ./env.sh

shmux -c 'docker ps -a' $MASTER $WORKERS 
