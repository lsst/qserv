set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
cd "${DIR}/.."
. "env.sh"

shmux -c "sudo -S sh -c 'systemctl daemon-reload && service docker restart'" $SSH_MASTER $SSH_WORKERS
