set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
cd "${DIR}/.."
. "env.sh"

REMOTE_DIR="/qserv/log"

shmux -c "sudo -S sh -c 'mkdir -p \"$REMOTE_DIR\" && chown -R qserv:qserv \"$REMOTE_DIR\"'" $SSH_MASTER $SSH_WORKERS
