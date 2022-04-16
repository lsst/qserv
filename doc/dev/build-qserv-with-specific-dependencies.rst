.. _build-qserv-with-specific-dependencies:

#########################################
Building Qserv with specific dependencies
#########################################

You may want to build Qserv against a specific set of
dependencies. Several solutions are available:

.. note::

   Commands below require that you've already sourced an eups environment.

************************************
Setup dependencies after Qserv setup
************************************

.. code-block:: bash

   cd ${SRC_DIR}/qserv
   setup -r .
   setup foo 1.2.3
   setup bar 3.4.5

***********************
Use setup --keep option
***********************

``--keep`` option will keep any products already setup (regardless of their
versions).

.. code-block:: bash

   setup foo 1.2.3
   setup bar 3.4.5
   cd ${SRC_DIR}/qserv
   setup --keep -r .

*******************************************************
Save the current EUPS environment, and restore it later
*******************************************************

.. code-block:: bash

   eups list -s > foo.tag # saves currently setup-ed products
   ...
   cd ${SRC_DIR}/qserv
   setup -t foo.tag -r .  # sets up the product with dependencies in file foo.tag

See https://dev.lsstcorp.org/trac/wiki/EupsTips#Tags for details.

************************************************************
Keep the dependency version-to-be-used declared as 'current'
************************************************************

To declare a version of an installed product as 'current', do:

.. code-block:: bash

   eups declare -t current <product> <version>
   ...
   cd ${SRC_DIR}/qserv
   setup -r .

*************************************
Declare your own, personal, EUPS tags
*************************************

When there is a need to build a specific ticket against a very specific set of 
versions, you can use EUPS tags to manage that.

Specifically, you can declare your own, personal, EUPS tags, as described at: 
https://dev.lsstcorp.org/trac/wiki/EupsTips#Tags

Once you edit your ~/.eups/startup.py as described in there, you will be able to do things such as:

.. code-block:: bash

   eups declare foo 1.2.3.4 -t dm1234
   eups declare bar 5.6.7.8 -t dm1234

So when you're working on resolving DM-1234 that needs those specific versions,
you can set them up with:

.. code-block:: bash

   ...
   cd ${SRC_DIR}/qserv
   setup -t dm1234 -r .

EUPS expert argue that this is the preferred way to do this, when you need it.
