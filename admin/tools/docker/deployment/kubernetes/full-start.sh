#!/bin/bash

# Create K8s cluster and launch Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -O            Openstack setup, create ssh tunnel to access k8s master
  -h            This message

  Create k8s cluster and launch Qserv

EOD
}
set -x

# Get the options
while getopts hO c ; do
    case $c in
        C) OPENSTACK=true ;;
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
	usage
    exit 2
fi

"$DIR"/build-kubectl-image.sh
echo "Setup Kubernetes cluster and launch Qserv"
# require sudo access on nodes
"$DIR"/sysadmin/kube-destroy.sh
"$DIR"/sysadmin/kube-create.sh
"$DIR"/sysadmin/export-kubeconfig.sh

# Hack for Openstack
if [ $OPENSTACK = true ]; then
    "$DIR/kubectl/ssh-tunnel.sh"
    sed -i -- 's,server: https://.*\(:[0-9]*\),server: https://localhost\1,g' \
        "$HOME"/.lsst/qserv-cluster/kubeconfig
fi

# require access to kubectl configuration
"$DIR"/start.sh
