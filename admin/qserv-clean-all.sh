#!/bin/bash

# Don't affect $QSERV_BASE/build

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

export QSERV_CLEAN_ALL=1
export LOG_FILE_PREFIX="CLEAN"
${QSERV_SRC}/admin/qserv-build-install-cmd-with-opts.sh
unset QSERV_CLEAN_ALL

#if [ -n "${QSERV_LOG}" ]; then
#	rm -r ${QSERV_LOG}/*
#fi
#if [ -n "${QSERV_MYSQL_DATA}" ]; then
#	rm -r ${QSERV_MYSQL_DATA}/*
#fi

#if [ -n "${QSERV_MYSQL_DATA}" ]; then
#	files="bin etc include INSTALL-*.log lib lib64 libexec man mysql-test qserv share sql-bench var xrootd xrootd-run" 
#	cd ${QSERV_BASE}
#	for f in $files 
#	do
#		rm -r $f
#	done
#	rm -f start_*
#fi

