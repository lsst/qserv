#!/bin/sh
set -x
## The functionality for this should be in workerMgmt.sh,
##  so use that version instead.
#exportroot=/tmp/exportTest  # match oss.localroot
exportroot=<XROOTD_RUN_DIR>  # match oss.localroot

# mysql bin location
#mysqlBin=/usr/bin/mysql
mysqlBin=<QSERV_BASE_DIR>/bin/mysql
#socketfile=$MYSQL_UNIX_PORT
#socketfile=/u1/local/mysql.sock
socketfile=<QSERV_BASE_DIR>/var/lib/mysql/mysql.sock

mysqlDbCmd="$mysqlBin --socket $socketfile -u root -p<MYSQLD_PASS>"

function fixDirForDb {
    local qservDb=$1
    # q, result should be exported in xrootd config file (all.export)
    qdir=$exportroot/q/$qservDb
    echo Removing existing $qdir 
    rm -rf $qdir 
    echo Making new $qdir 
    mkdir -p $qdir 
    echo Making placeholders
    echo "show tables in $qservDb;" | $mysqlDbCmd | grep Object_ | sed "s#Object_\(.*\)#touch $qdir/\1 ;#" | sh
}

function fixResultDir {
    rdir=$exportroot/result
    echo Removing existing $rdir
    rm -rf  $rdir
    echo making sure result dir exists
    mkdir -p $rdir
}

fixDirForDb LSST
fixResultDir
