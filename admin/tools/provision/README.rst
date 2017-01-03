**************************************
Provision Qserv using Docker+Openstack
**************************************

Pre-requisite
-------------

* Install system dependencies

.. code-block:: bash

   sudo apt-get install python-dev python-pip

   # Might be required on Ubuntu 14.04
   pip install --upgrade --force pbr

   # Install the OpenStack client
   sudo pip install python-openstackclient

* Download Openstack RC file: http://docs.openstack.org/user-guide/common/cli_set_environment_variables_using_openstack_rc.html

* Install shmux to run multi node tests
   see http://web.taranis.org/shmux/


Provision Qserv & run multinode tests
-------------------------------------

* Clone Qserv repository and set Openstack environment and parameters:

.. code-block:: bash

   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   git clone https://github.com/lsst/qserv.git
   cd qserv/admin/tools/provision

   # Source Openstack RC file
   # This is an example for NCSA
   . ./LSST-openrc.sh

   # Update the configuration file which contains instance parameters
   # Add special tuning if needed
   cp LSST.example.conf <OS_PROJECT_NAME>.conf

* Create customized image, provision openstack cluster and run integration tests

.. code-block:: bash

    # Use -h to see all available options
    ./test.sh

.. warning::
   If `test.sh` crashes during integration tests with shmux,
   your original `~/.ssh/config` might have been moved in `~/.ssh/config.backup`

