#!/bin/sh

BASEPATH=/u1/qserv/ticket1944-qms_run/meta

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/python

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib
$PYTHON $BASEPATH/bin/startQMS.py -c $BASEPATH/examples/qmsConfig.cnf
