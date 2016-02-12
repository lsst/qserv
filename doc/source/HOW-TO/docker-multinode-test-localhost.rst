############################################################
Run multinode test inside Docker containers on a workstation
############################################################

.. note::

   This procedure was tested with Docker 1.9.

**************
Pre-requisites
**************

- A Qserv master and a worker image made from the github branch/tag which will be tested, see :ref:`docker-github`

- A user account on a Linux workstation, for example: account *myuser* on *myhost.domain.org*
- Internet access available for *myhost.domain.org*
- Docker running on *myhost.domain.org*
- *myuser* belonging to  *docker* group on *myhost.domain.org*

  .. code-block:: bash

     sudo usermod -a -G docker myuser

******************
Run multinode test
******************

On the workstation *myhost.domain.org*, clone Qserv code and go to directory containing tests scripts.

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd qserv/admin/tools/docker/integration-tests/localhost

In :file:`env.sh`, set the name of your image, based on the github branch name:

.. literalinclude:: ../../../admin/tools/docker/integration-tests/localhost/env.example.sh
   :language: bash
   :linenos:

:file:`run-multinode-tests.sh` downloads docker images and runs multinode tests:

.. code-block:: bash

   ./run-multinode-tests.sh


:file:`pull.sh` download an update of Docker containers created using procedure described here: :ref:`docker-github`

.. code-block:: bash

   ./pull.sh

