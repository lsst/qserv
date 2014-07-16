#!/usr/bin/env sh

QSERV_DIR=%(QSERV_DIR)s
HOME=%(HOME)s

# CSS needs MySQL access
cp ${QSERV_DIR}/tmp/configure/my.cnf ${HOME}/.my.cnf
