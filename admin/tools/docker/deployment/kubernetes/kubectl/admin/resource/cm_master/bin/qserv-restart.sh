#!/bin/sh

/qserv/run/bin/qserv-stop.sh && /qserv/run/bin/qserv-start.sh

# ideally, status including pid should be printed from inside qserv-start.sh
/qserv/run/bin/qserv-status.sh
