#!/bin/bash
# TODO manage scisql version in templating system
cd %(QSERV_BASE_DIR)s/build &&
tar jxvf scisql-0.3.tar.bz2 &&
cd scisql-0.3 &&
./configure --prefix %(QSERV_BASE_DIR)s --mysql-user root --mysql-socket %(MYSQLD_SOCK)s/var/lib/mysql/mysql.sock &&
make &&
make install
