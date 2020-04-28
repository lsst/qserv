set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
cd "${DIR}/.."
. "env.sh"

shmux -c 'echo "Connected to $(hostname) on $(date)"' $SSH_MASTER $SSH_WORKERS
