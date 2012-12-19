#!/bin/sh

# You need to be in the qserv directory (top level).
BASEPATH=`pwd`

export PYTHONPATH=$BASEPATH/admin/dist

# Use OS-default python, not SLAC /usr/local/bin/python
PYTHON=/usr/bin/python

# Run the script
$PYTHON $BASEPATH/admin/bin/qmwTool.py $*
