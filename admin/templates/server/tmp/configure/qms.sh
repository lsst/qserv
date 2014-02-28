#!/bin/bash
%(QSERV_DIR)s/etc/init.d/mysqld start && 
cd %(QSERV_SRC_DIR)s/meta &&
scons --site-dir=%(QSERV_SRC_DIR)s/site_scons &&
%(QSERV_DIR)s/bin/mysql -vvv --socket %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s < %(QSERV_DIR)s/tmp/qms_qmsdb.sql &&
# %(QSERV_DIR)s/bin/mysqladmin -S %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s shutdown &&
cp %(QSERV_DIR)s/tmp/qms_admclient.cnf %(HOME)s/.lsst/qmsadm
%(QSERV_DIR)s/etc/init.d/mysqld stop 
