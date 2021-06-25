#!/bin/sh
#
# qserv-wmgr   This script starts Qserv worker management service.
#
# description: Start and stop qserv worker management service.
#
# In fact, this script is a only wrapper for real wmgr startup script.
# This allows users to open a shell as root inside the container whereas
# wmgr run using 'qserv' account.

set -e
set -x

# Launch service
#
su qserv -c "sh /config-start/wmgr.sh"
