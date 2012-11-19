#mysql --host clrlsst-dbmaster.in2p3.fr --port 4040 --user qsmaster LSST 2>&1 > qserv-test.out < qserv-test.sql&
source $QSERV_SRC/qserv-install-params.sh
mysql --host 127.0.0.1 --port $QSERV_MYSQL_PROXY_PORT --user qsmaster LSST 
