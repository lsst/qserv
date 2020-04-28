#!/bin/sh

# Display <DATA_DIR> usage on all nodes
# User must have sudo access on all nodes

# @author Fabrice Jammes IN2P3

DATA_DIR=/qserv

if [ -n "$PASSWORD" ]
then
    echo "Display $DATA_DIR usage on all nodes"
    parallel --nonall --slf .. \
      'echo Running on $(hostname) && echo "'$PASSWORD"\" | sudo -S -p '' sh -c 'du -skh \"$DATA_DIR\" && du -skh \"$DATA_DIR\"/*'"
else
   echo "export PASSWORD to run this script"
fi
