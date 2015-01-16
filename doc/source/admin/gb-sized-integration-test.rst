*******************************
Run a GB-sized integration test
*******************************

Create a GB-sized dataset
=========================

Once a Qserv mono-node instance is configured, you can automatically download a GB-sized
dataset from a remote server using next command:

.. warning::

    This part require rsync and a ssh key to access lsst-dev.ncsa.illinois.edu.

.. code-block:: bash

   qserv-check-integration.py --download --work-dir=/path/to/large/storage --case-id=04 --custom-case-id=<new-id>
   # See advanced options with --help option

Here data will be duplicated from built-in test case #04, located in :file:`$QSERV_TESTDATA_DIR/datasets`,
to :file:`/path/to/large/storage/case<new-id>`, and some original data files will be overridden with big remote data files.

.. note::

    Built-in test datasets are good models if you want to create your own custom test dataset by hand.

Run integration test on a custom dataset
========================================

First start Qserv and then:

.. code-block:: bash

   qserv-test-integration.py --load --testdata-dir=/path/to/large/storage --case-id=<new-id>
   # See advanced options with --help option

This test will create a Qserv database named ``qservTest_case<new-id>_qserv``, and
a plain MySQL database named ``qservTest_case<new-id>_mysql``.
Results are stored in :file:`QSERV_RUN_DIR/tmp/qservTest_case<new-id>/outputs/`, and are erased before each run.

