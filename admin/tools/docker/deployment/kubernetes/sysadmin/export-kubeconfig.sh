#!/bin/sh

# Export kubectl configuration 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

KUBECONFIG="$HOME/.lsst/qserv-cluster/kubeconfig"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -K            Path to exported KUBECONFIG file, default to $KUBECONFIG
  -h            This message

  Export kubectl configuration from k8s master

EOD
}
set -x

# Get the options
while getopts hK: c ; do
    case $c in
        K) KUBECONFIG="${OPTARG}" ;;
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
	usage
    exit 2
fi

case "$KUBECONFIG" in
    /*) ;;
    *) echo "expect absolute path" ; exit 2 ;;
esac      

# strip trailing slash
KUBECONFIG=$(echo $KUBECONFIG | sed 's%\(.*[^/]\)/*%\1%')

echo "WARN: require sudo access to $ORCHESTRATOR"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" 'sudo cat /etc/kubernetes/admin.conf' \
	> "$KUBECONFIG"
