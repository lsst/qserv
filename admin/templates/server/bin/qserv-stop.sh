#!/bin/sh

QSERV_DIR=%(QSERV_DIR)s
. ${QSERV_DIR}/bin/env.sh

services_rev=`echo ${SERVICES} | tr ' ' '\n' | tac`
for service in $services_rev; do
    ${QSERV_DIR}/etc/init.d/$service stop
done
# still usefull ?
rm -f ${QSERV_RUN}/xrootd-run/result/*

