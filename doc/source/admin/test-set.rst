*********
Test sets
*********

Integration tests
=================

Once a Qserv mono-node instance is running, you can run advanced integration test on one dataset by using:

.. code-block:: bash

   qserv-check-integration.py --load --case=01
   # See advanced options with --help option

You can also run the whole integration test suite, with fixed parameters by using :

.. code-block:: bash

   qserv-test-integration.py
   # See advanced options with --help option

Results are stored in ``$QSERV_RUN_DIR/tmp/qservTest_case<number>/outputs/``, and are erased before each run.

Input data sets
---------------

Directory structure
^^^^^^^^^^^^^^^^^^^

::

    case<number>/
        README.txt - contains info about data
        queries/
        data/
        <table>.schema - contains schema info per table
        <table>.csv.gz - contains data``

Database
^^^^^^^^

data from case<number> will be loaded into databases called
 - ``qservTest_case<number>_mysql``, for mysql
 - and ``LSST``, for qserv

Query file format
^^^^^^^^^^^^^^^^^

Query file are named ``<idA>_<descr>.sql`` where ``<idA>`` means :
  - ``0xxx`` supported, trivial (single object)
  - ``1xxx`` supported, simple (small area)
  - ``2xxx`` supported, medium difficulty (full scan)
  - ``3xxx`` supported, difficult /expensive (e.g. full sky joins)
  - ``4xxx`` supported, very difficult (eg near neighbor for large area)
  - ``8xxx`` queries with bad syntax. They can fail, but should not crash the server
  - ``9xxx`` unknown support

Files that are not yet supported should have extension ``.FIXME``.
