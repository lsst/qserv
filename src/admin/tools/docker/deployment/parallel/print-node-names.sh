#!/bin/sh

# Print cluster nodes name

# @author  Fabrice Jammes, IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo
echo "Master and Workers"
echo "=================="
echo
echo "$MASTER"
echo "$WORKERS"
echo
echo "SSH Master and Workers"
echo "======================"
echo
echo "$SSH_MASTER"
echo "$SSH_WORKERS"
