SERVICES="mysqld xrootd zookeeper mysql-proxy qserv-czar"

usage() {
	echo "Usage: $0 [-r qserv_run_dir] [-h]"
	exit 1
}

check_qserv_run_dir() {

    [ -z ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR : Unable to start Qserv"
        echo "Please specify an execution (i.e. run) directory using -r option, or by running : "
        echo "    export QSERV_RUN_DIR=/qserv/run/dir/ "
        exit 1
    }

    [ ! -d ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR : Unable to start Qserv"
        echo "QSERV_RUN_DIR (${QSERV_RUN_DIR}) has to point on an existing directory"
        exit 1
    }

    [ ! -w ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR : Unable to start Qserv"
        echo "Write access required to QSERV_RUN_DIR (${QSERV_RUN_DIR})"
        exit 1
    }

    echo "Qserv execution directory : ${QSERV_RUN_DIR}"
}
