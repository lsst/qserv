###########################################
Run multinode test inside Docker containers 
###########################################

.. note::

   This procedure was tested with Docker 1.7 and 1.8.

**************
Pre-requisites
**************

- A user account on a handful of Linux machines, for example: account *myuser* on *myhost.in2p3.fr*, and *ccqserv00.in2p3.fr* to *ccqserv03.in2p3.fr*
- *ssh* access from *myhost.in2p3.fr* to all *qservXX.in2p3.fr* for *myuser* account, with no password prompt, for example using ssh keys authentication mechanism.
- A parallel *ssh* client installed on *myhost.in2p3.fr*, for example
  *shmux* (http://web.taranis.org/shmux/)
- Internet access for all *ccqservXX.in2p3.fr*
- Docker running on all *ccqservXX.in2p3.fr*
- Add *myuser* to *docker* group on all *ccqservXX.in2p3.fr*

  .. code-block:: bash

     sudo usermod -a -G docker myuser 
  
****************** 
Run multinode test
******************

On *myhost.in2p3.fr*, create and adapt next example scripts to prepare multinode test execution:

In :file:`env.sh`, prepare your host list:

.. literalinclude:: ../../../admin/tools/docker/shmux/env.sh
   :language: bash
   :linenos:
 
In :file:`nodes.example.css`, add worker nodes to css configuration:

.. literalinclude:: ../../../admin/tools/docker/shmux/nodes.example.css
   :linenos:

In :file:`run-multinode-tests.sh`, download docker images and run multinode test: 

.. literalinclude:: ../../../admin/tools/docker/shmux/run-multinode-tests.sh
   :language: bash
   :linenos:

Then, launch multinode test:

.. code-block:: bash

   ./run-multinode-tests.sh

