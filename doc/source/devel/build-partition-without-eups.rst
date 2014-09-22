#############################################
Building partition package without using eups
#############################################

Developer may be interested to build Qserv packages without having to install
eups stack. An example is given here with the partition package, whose build
system relies on sconsUtils.

**************
Pre-requisites
**************

Install scons v2.1.0 or greater.

*****
Build 
*****

* Install sconsUtils:
.. code-block:: bash

   # clone sconsUtils repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   git clone git://git.lsstcorp.org/LSST/DMS/devenv/sconsUtils

* Clone partition repository:
.. code-block:: bash

   # clone partition repository
   cd ${SRC_DIR}
   # anonymous access : 
   git clone git://git.lsstcorp.org/LSST/DMS/partition 
   # or authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/partition
   # build and install your Qserv version
   cd partition

* Retrieve sconsUtils configuration files for Boost: 
.. code-block:: bash

   git archive --remote=git://git.lsstcorp.org/LSST/external/boost --format=tar HEAD ups/*.cfg | tar xv

* Define build environment:
.. code-block:: bash

   export PYTHONPATH=${PYTHONPATH}:${SRC_DIR}/sconsUtils/python/
   export PARTITION_DIR=${SRC_DIR}/partition/
   # Variables below are not required if you use system-provided Boost libraries
   # define them only if you use you own Boost libraries :
   export BOOST_DIR=dir/where/your/own/boost/is/installed
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$BOOST_DIR/lib

* Build:

   scons
 
