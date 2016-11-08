set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
PARENT_DIR=$(dirname "$DIR")

. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"

for node in "$MASTER" $WORKERS
do
    ssh -t "$node" "sudo sh -c \"sed -i -e 's/Defaults    requiretty.*/#Defaultsrequiretty/g' \
		/etc/sudoers\""
done
