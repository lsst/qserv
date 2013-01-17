#!/bin/sh

echo 'This needs qms working, and the db "Summer2012" should be registered'

## this is the command that will register it:
## ./meta/examples/runMetaClientTool.sh createDb Summer2012 @./meta/examples/dbPartitioned.params

echo '=== destroyMeta ==='
./admin/examples/run_qmwTool -v destroyMeta

echo '=== installMeta ==='
./admin/examples/run_qmwTool -v installMeta

echo '=== printMeta ==='
./admin/examples/run_qmwTool -v printMeta

echo '=== registerDb (missing arg) ==='
./admin/examples/run_qmwTool -v registerDb

echo '=== registerDb (db not in qms) ==='
./admin/examples/run_qmwTool -v registerDb xyzdd

echo '=== registerDb (all good) ==='
./admin/examples/run_qmwTool -v registerDb Summer2012
