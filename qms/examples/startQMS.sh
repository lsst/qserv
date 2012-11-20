#!/bin/sh

BASEPATH=/u1/qserv/ticket1944-qms_run

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/qms/python

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python
export LD_LIBRARY_PATH=/u1/lsst/lib
$PYTHON $BASEPATH/qms/bin/startQMS.py -c $BASEPATH/qms/examples/qmsConfig.cnf
