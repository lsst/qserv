#!/bin/sh

# Cleanup /qserv directory
# User must have sudo access on all nodes

# @author Fabrice Jammes IN2P3

DELETE_FILES="/qserv/qserv_client_error.log \
    /qserv/scripts /qserv/setup-scripts /qserv/var"

DIR=$(cd "$(dirname "$0")"; pwd -P)

if [ -n "$PASSWORD" ]
then
    echo "Delete $DELETE_FILES on all nodes"
    parallel --nonall --slf .. \
      'echo Running on $(hostname) && echo "'$PASSWORD"\" | sudo -S -p '' sh -c 'rm -rf $DELETE_FILES'"
else
   echo "export PASSWORD to run this script"
fi
