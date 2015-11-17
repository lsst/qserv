qserv_node_type="{{NODE_TYPE}}"
case "$qserv_node_type" in
    mono|master) SERVICES="mysqld xrootd mysql-proxy qserv-watcher qserv-wmgr" ;;
    worker) SERVICES="mysqld xrootd qserv-wmgr" ;;
esac

check_qserv_run_dir() {

    [ ! -w ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR: Unable to start Qserv"
        echo "ERROR: Write access required to QSERV_RUN_DIR (${QSERV_RUN_DIR})"
        exit 1
    }

    echo "INFO: Qserv execution directory : ${QSERV_RUN_DIR}"
}
