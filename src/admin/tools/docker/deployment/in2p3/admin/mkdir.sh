#!/bin/sh

# Cleanup /qserv directory
# User must have sudo access on all nodes

# @author Fabrice Jammes IN2P3

NEW_DIR=/qserv/custom

if [ -n "$PASSWORD" ]
then
    echo "Create $NEW_DIR on all nodes"
    parallel --nonall --slf .. \
      'echo Running on $(hostname) && echo "'$PASSWORD"\" | sudo -S -p '' sh -c 'mkdir -p $NEW_DIR && chown qserv:qserv $NEW_DIR'"
else
   echo "export PASSWORD to run this script"
fi
