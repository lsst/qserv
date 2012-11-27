#!/bin/bash

# NEED TO BE RUNNED AS ROOT

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

# patching /etc/my.cnf
if [ -f /etc/redhat-release ]
    then
    perl -i.bak -pe 's/user=mysql//' /etc/my.cnf;
elif [ -f /etc/debian_version ]
    then
    perl -i.bak -pe 's/user=mysql//' /etc/mysql/my.cnf;
fi

echo "Creating ${QSERV_BASE} directory"
mkdir -p ${QSERV_BASE} || die "mkdir failed";
chown -R ${QSERV_USER}:${QSERV_USER} ${QSERV_BASE} || die "chown failed"; 

if [ -n "${QSERV_LOG}" ]; then
	echo "Creating ${QSERV_LOG} directory"
	mkdir -p ${QSERV_LOG} || die "mkdir failed";
	chown -R ${QSERV_USER}:${QSERV_USER} ${QSERV_LOG} || die "chown failed";
fi
if [ -n "${QSERV_MYSQL_DATA}" ]; then
	echo "Creating ${QSERV_MYSQL_DATA} directory"
	mkdir -p ${QSERV_MYSQL_DATA} || die "mkdir failed";
	chown -R ${QSERV_USER}:${QSERV_USER} ${QSERV_MYSQL_DATA} || die "chown failed";
fi
