if [ -z "${QSERV_ENV_SETTED}" ]; then
	# in order to load numpy
	export PYTHONPATH=${PYTHONPATH}:/usr/lib64/python2.6/site-packages/
	export QSERV_ENV_SETTED=1
fi 
    
QSERV_DIR=%(QSERV_DIR)s
SERVICES="mysqld xrootd qms mysql-proxy qserv-master"

qserv-start() {
    for service in ${SERVICES}; do
        ${QSERV_DIR}/etc/init.d/$service start
    done
}


qserv-status() {
    services="mysqld xrootd qms mysql-proxy qserv"
    for service in ${SERVICES}; do
        ${QSERV_DIR}/etc/init.d/$service status
    done
}

qserv-stop() {
    services_rev=`echo ${SERVICES} | tr ' ' '\n' | tac`
    for service in $services_rev; do
        ${QSERV_DIR}/etc/init.d/$service stop
    done
    # still usefull ?
    rm -f ${QSERV_RUN}/xrootd-run/result/*
}

