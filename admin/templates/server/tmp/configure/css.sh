#!/usr/bin/env sh

QSERV_DIR=%(QSERV_DIR)s
HOME=%(HOME)s

cp ${QSERV_DIR}/tmp/configure/my.cnf ${HOME}/.my.cnf
