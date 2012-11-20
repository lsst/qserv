#!/bin/sh

XRD_DIR=/u1/qserv/xrootd
PLATFORM=x86_64_linux_26_dbg

BASEPATH=/u1/qserv/ticket1944-qms

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/metadata/python

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib:$XRD_DIR/lib/$PLATFORM
$PYTHON $BASEPATH/metadata/bin/startQMS.py -c $BASEPATH/metadata/examples/qmsConfig.cnf
