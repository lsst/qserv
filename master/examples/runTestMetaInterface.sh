#!/bin/sh
XRD_DIR=/u1/qserv/xrootd
PLATFORM=x86_64_linux_26_dbg

BASEPATH=/u1/qserv/qserv

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/master/python

# export QSERV_CONFIG=$BASEPATH/master/examples/qmsConfig.cnf

# dont forget to grant access for the qservMetadata database
# GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE ON qservMetadata.* TO 'qsmaster'@'localhost' WITH GRANT OPTION;

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib:$XRD_DIR/lib/$PLATFORM
export QSW_RESULTDIR=/u1/qserv-run/tmp
$PYTHON $BASEPATH/master/python/lsst/qserv/metadata/testMetaInterface.py
