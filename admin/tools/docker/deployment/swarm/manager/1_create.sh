#!/bin/sh

# Configure a swarm cluster for Qserv

# @author  Fabrice Jammes, IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

QSERV_NETWORK="qserv"
# Create a swarm on the openstack machine dedicated to swarm
HOST_IP=$(hostname --ip-address)
docker swarm init --advertise-addr "$HOST_IP" --listen-addr "$HOST_IP"

# Create swarm network to enable communication between containers
MTU=$(cat /sys/class/net/eth0/mtu)
docker network create  -o "com.docker.network.driver.mtu"="$MTU" --driver overlay "$QSERV_NETWORK"
