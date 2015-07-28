#!/bin/sh

{{QSERV_RUN_DIR}}/bin/qserv-stop.sh && {{QSERV_RUN_DIR}}/bin/qserv-start.sh

# ideally, status including pid should be printed from inside qserv-start.sh
{{QSERV_RUN_DIR}}/bin/qserv-status.sh
