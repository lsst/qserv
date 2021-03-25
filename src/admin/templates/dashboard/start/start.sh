#!/bin/sh

# Start nginx inside pod
# and do not exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -euxo pipefail

nginx -c /config-etc/nginx.conf
