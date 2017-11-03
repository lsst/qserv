#!/bin/sh

# Run docker container containing kubectl tools and scripts

# @author  Fabrice Jammes

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

# IN2P3
KUBECONFIG_DEFAULT_1="/qserv/kubernetes"
# Openstack
KUBECONFIG_DEFAULT_2="$HOME/.lsst/qserv-cluster"
if [ -x "$KUBECONFIG_DEFAULT_1" ]; then
    KUBECONFIG="$KUBECONFIG_DEFAULT_1"
elif [ -x "$KUBECONFIG_DEFAULT_2" ]; then
    KUBECONFIG="$KUBECONFIG_DEFAULT_2"
fi
echo $KUBECONFIG
     

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -C            Command to launch inside container
  -K            Path to configuration directory,
                default to $KUBECONFIG_DEFAULT_1 if readable
                if not default to $KUBECONFIG_DEFAULT_2 if readable
  -h            This message

Run docker container containing k8s management tools (helm,
kubectl, ...) and scripts.

EOD
}
set -x

# Get the options
while getopts hC:K: c ; do
    case $c in
        C) CMD="${OPTARG}" ;;
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

if [ ! -r "$KUBECONFIG" ]; then
    echo "ERROR: incorrect KUBECONFIG file: $KUBECONFIG"
    exit 2
fi

if [ -z "${CMD}" ]
then
	BASH_OPTS="-it --volume "$DIR"/kubectl/admin:/root/admin-dev"
    CMD="bash"
fi

# Launch container
#
# Use host network to easily publish k8s dashboard
IMAGE=qserv/kubectl
docker pull "$IMAGE"
docker run $BASH_OPTS --net=host \
    --rm \
    --volume "$KUBECONFIG"/kubeconfig:/root/.kube/config \
    --volume "$KUBECONFIG"/env.sh:/root/.kube/env.sh \
    --volume "$KUBECONFIG"/env-infrastructure.sh:/root/.kube/env-infrastructure.sh \
    "$IMAGE" $CMD
