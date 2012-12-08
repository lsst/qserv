#!/bin/sh

BASEPATH=`pwd`

export PYTHONPATH=/u1/lsst/lib/python2.5/site-packages:$BASEPATH/meta/python

PYTHON=/usr/bin/python # Use OS-default python, not SLAC /usr/local/bin/python

$PYTHON $BASEPATH/meta/tests/completeTest.py
