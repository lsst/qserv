*******************************************
How to configure a multi-node Qserv cluster
*******************************************

*Note: this document uses examples of a 2-worker node cluster*

Pre-requisites
==============

Build/install Qserv and its pre-requisites on all nodes.

Configure master and worker nodes
=================================

1. Set up environment on master and all workers:

.. code-block:: bash

  source <path to stack>/loadLSST.bash
  cd <path to respositories>
  setup -r qserv
  setup -r qserv_testdata -k

2. Configure Qserv on master and all workers:

- Run :code:`qserv-configure.py -i -R <qserv-run directory>`

- Edit :code:`<qserv-run directory>/qserv-meta.conf`
 
  - Change :code:`node_type` to :code:`master` or :code:`worker`
  - | Change master DNS name to the name of the master machine as shown by :code:`hostname -s`.
    | Do not alter it in any way, for example, do not add the full domain name.
    | On the worker, use the master's DNS name, **not** the worker's

- Rerun :code:`qserv-configure.py` without :code:`-i:` :code:`qserv-configure.py -R <qserv-run directory>`

3. Copy secret file from the master to every worker:

   *Note: secret file is read when qserv-wmgr client starts*

   :code:`scp <qserv-run directory>/etc/wmgr.secret <worker1 hostName>:<qserv-run directory>/etc/wmgr.secret`

   :code:`scp <qserv-run directory>/etc/wmgr.secret <worker2 hostName>:<qserv-run directory>/etc/wmgr.secret`

4. Start services on master and every worker:

   :code:`<qserv-run directory>/bin/qserv-start.sh`

5. Grant permissions on every worker:

   :code:`mysql -S <qserv-run directory>/var/lib/mysql/mysql.sock -uroot -p`

   .. code-block:: sql

     GRANT USAGE ON *.* TO 'qsmaster'@'<master hostName>';
     GRANT SELECT ON `mysql`.* TO 'qsmaster'@'<master hostName>';
     GRANT ALL PRIVILEGES ON `qservTest%`.* TO 'qsmaster'@'<master hostName>';
     GRANT ALL PRIVILEGES ON `qservw_worker`.* TO 'qsmaster'@'<master hostName>';

6. On the master, register workers with CSS:

   :code:`qserv-admin.py`

   .. code-block:: none

     CREATE NODE worker1 type=worker port=5012 host=<worker1 hostName>;
     CREATE NODE worker2 type=worker port=5012 host=<worker2 hostName>;

Run Integration Test
====================

.. code-block:: bash

  qserv-test-integration.py

Or for individual test cases:

.. code-block:: bash

  qserv-check-integration.py --case=01 --load
  qserv-check-integration.py --case=02 --load
  ...
