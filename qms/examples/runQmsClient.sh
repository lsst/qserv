#!/bin/sh

BASEPATH=`pwd`

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/../python

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib
export QSW_RESULTDIR=/u1/qserv-run/tmp
$PYTHON $BASEPATH/../python/lsst/qserv/qms/qmsClient.py $*
