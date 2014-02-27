#!/bin/bash
%(QSERV_BASE_DIR)s/etc/init.d/mysqld start && 
cd %(QSERV_SRC_DIR)s/meta &&
scons --site-dir=%(QSERV_SRC_DIR)s/site_scons &&
%(QSERV_BASE_DIR)s/bin/mysql -vvv --socket %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s < %(QSERV_BASE_DIR)s/tmp/qms_qmsdb.sql &&
# %(QSERV_BASE_DIR)s/bin/mysqladmin -S %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s shutdown &&
cp %(QSERV_BASE_DIR)s/tmp/qms_admclient.cnf %(HOME)s/.lsst/qmsadm
%(QSERV_BASE_DIR)s/etc/init.d/mysqld stop 
