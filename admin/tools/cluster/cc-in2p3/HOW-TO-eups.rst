#################################################
HOW-TO use a custom version of Qserv on all nodes
#################################################

On the build node:

.. code-block:: bash

   # Update acl
   # TODO: test it
   setfacl --recursive --modify g::rwx /sps/lsst/Qserv/stack/Linux64/qserv

And then for each developer:

.. code-block:: bash

   # Stack path must be the same on all nodes
   source /qserv/stack/loadLSST.bash
   cd ~/src/qserv/
   eupspkg -erd install
   # Add latestbuild tag to $EUPS_PATH/ups_db/global.tags, if not exists
   grep -q -F 'latestbuild' $EUPS_PATH/ups_db/global.tags || echo 'latestbuild' >> $EUPS_PATH/ups_db/global.tags
   # declare Qserv
   eupspkg -erd decl -t latestbuild

And eventually update ~/.eups/startup.py on all the cluster nodes.
