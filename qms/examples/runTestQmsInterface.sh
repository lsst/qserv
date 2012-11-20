#!/bin/sh

BASEPATH=/u1/qserv/qserv

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/qms/python

# export QSERV_CONFIG=$BASEPATH/qms/examples/qmsConfig.cnf

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib
export QSW_RESULTDIR=/u1/qserv-run/tmp
$PYTHON $BASEPATH/qms/python/lsst/qserv/qms/testQmsInterface.py
