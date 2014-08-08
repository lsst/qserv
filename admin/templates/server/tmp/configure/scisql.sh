#!/usr/bin/env bash

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
MYSQL_DIR={{MYSQL_DIR}}
MYSQLD_SOCK={{MYSQLD_SOCK}}
MYSQLD_USER={{MYSQLD_USER}}
MYSQLD_PASS={{MYSQLD_PASS}}
PYTHON_BIN={{PYTHON_BIN}}
export PYTHONPATH={{PYTHONPATH}}

PYTHON_DIR=`dirname ${PYTHON_BIN}`
export PATH=${PYTHON_DIR}:${PATH}
export LD_LIBRARY_PATH={{LD_LIBRARY_PATH}}

# TODO manage scisql version in templating system
SCISQL_VERSION=scisql-0.3.2
SCISQL_ARCHIVE=${SCISQL_VERSION}.tar.bz2

${QSERV_RUN_DIR}/etc/init.d/mysqld start &&

cd ${QSERV_RUN_DIR}/tmp &&
if [ ! -s ${SCISQL_ARCHIVE} ]; then
    curl -O -L https://launchpad.net/scisql/trunk/0.3.2/+download/${SCISQL_ARCHIVE} ||
    {
        echo "Unable to download ${SCISQL_ARCHIVE}"
        echo "Please copy it manually in ${QSERV_RUN_DIR}/tmp"
        exit 1
    }
fi
tar jxvf ${SCISQL_ARCHIVE} &&
cd ${SCISQL_VERSION} &&
echo "-- Installing scisql $PYTHONPATH"
./configure --prefix=${MYSQL_DIR} --mysql-user=root --mysql-password="${MYSQLD_PASS}" --mysql-socket=${MYSQLD_SOCK}  &&
make &&
make install &&
${QSERV_RUN_DIR}/etc/init.d/mysqld stop ||
exit 1

echo "INFO: SciSQL installation and configuration SUCCESSFUL"
