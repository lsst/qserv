#!/bin/bash

set -e
set -x

QSERV_RUN_DIR=$HOME/qserv-run

. $HOME/stack/loadLSST.bash
setup qserv_distrib -t qserv
qserv-configure.py --all --force -R $QSERV_RUN_DIR
$QSERV_RUN_DIR/bin/qserv-start.sh
qserv-test-integration.py
$QSERV_RUN_DIR/bin/qserv-stop.sh
