#!/bin/sh

QSERV_DIR=%(QSERV_DIR)s
source ${QSERV_DIR}/bin/env.sh

for service in ${SERVICES}; do
    ${QSERV_DIR}/etc/init.d/$service status
done

 
