******************
Data Loader Script
******************

Overview
========

Data loader script (qserv-data-loader.py) was developed primarily to simplify
implementation of the data integration test, but eventually this script may
evolve into full-featured data loading solution supporting production data
loading. The script currently supports data loading into both partitioned and
non-partitioned tables. It also updates CSS with the information about created
tables or data chunks.

Data loader works with one database table at a time and normally does not
provide any support for database-level operations (one exception to that is the
creation of per-database information in CSS if that information does not exist
yet). Before loading data into MySQL the database in MySQL must exist already
and proper permissions should be granted to the account used to load data.

At the very high level data loader operation can be summarized as:

* reading table configuration parameters
* uncompressing compressed data files
* partitioning data if necessary
* loading partitioned data into databases
* updating meta-data in the databases
* updating CSS information for loaded data

Data loader supports different operating modes controlled by command line
options:

* *mono-node* - data for partitioned tables is partitioned (or
  pre-partitioned) and loaded into individual chunk and overlap tables, data
  for non-partitioned tables is loaded into a separate (non-chunked) tables.
  All data is stored in one database on a single node.
* *multi-node* - same as above but data are stored in multiple databases
  across several worker nodes, there is a copy of data for non-partitioned
  tables on each worker node.
* *one-table* - all data (partitioned or non-partitioned) is stored in one
  non-chunked table in single database. Partitioned tables may have their data
  pre-partitioned but all chunks are still merged into one table. This is
  useful primarily for integration test for comparison of query results between
  partitioned and non-partitioned data.


Configuration Parameters
========================

Data loader behavior is determined by large number of options which come either
from command line or configuration files. The location of the configuration
files is determined by the command line options (``-f`` or ``--config``).
Script can load multiple configuration files, typically there would be one
common configuration files with per-database options and one file per table
with table-specific options, and usually only partitioned tables need per-table
options.

Configuration file format and a subset of parameters is shared with partitioner,
for detailed description of all supported parameters see (TODO)


Temporary Data Location
=======================

The script may produce temporary files as the result of uncompressing compressed
data or partitioning data for partitioned tables. The size of temporary files
can be large (possibly much larger than original data if data is compressed).
Chunks files from partitioning operation are stored in a location determined by
``--chunks-dir`` command line option which defaults to ``./loader_chunks``. The
directory does not have to exist and will be created if necessary. If directory
already exists then it must be empty unless ``--skip-partition`` option is
given.

If ``--tmp-dir`` option is specified then temporary uncompressed files will be
stored at that location, otherwise uncompressed files will be stored inside
chunks directory (or its sub-directory with random name). At the end of the run
directory with uncompressed data will be removed by the script unconditionally,
directory with chunk files will be removed unless option ``--keep-chunks`` is
specified on a command line.


Partitioning Data
=================

For partitioned tables (this is determined by parameters in configuration files)
script will run partitioner on input (or uncompressed) data which will write
chunk files into a temporary directory. Script will later search that directory
for all chunk and overlap files and load those files into separate
chunk/overlap tables in database(s).

It is possible to run script on data which is already pre-partitioned (e.g. by
previous run of the same script with ``--keep-chunks`` option or by
duplicator). To use existing data pass ``--skip-partition`` option and specify
the location of chunk files with ``--chunks-dir`` option. It is caller
responsibility to make sure that data is partitioning was done consistently
with the parameters specified in configuration files.

In *one-table* mode when data need to be loaded into one table the script needs
``--one-table`` option. If both ``--skip-partition`` and ``--one-table`` are
specified then original (uncompressed) data is loaded into a tables, without
``--skip-partition`` data is partitioned first but then all resulting chunks
are merged into one table.

Non-partitioned tables do not require partitioning.


Mono-node vs Multi-node
=======================

In *mono-node* setup (current standard mode for integration tests) there is a
single database shared between czar and worker. Database connection and
authorization for this single database instance parameters are determined by
command line options.

In *multi-node* setup there is a single database instance used by czar and one
or more worker databases. Connection parameters for czar database are
determined from command line options, connection parameters for worker
databases are normally defined in CSS (via ``qserv-admin.py`` script
``CREATE NODE`` command). The set of worker nodes used for data loading is
controlled by ``--worker`` options to the script. It is essential that the same
set of workers is specified for all tables in a database.

In *multi-node* mode chunks of the partitioned tables are distributed across all
workers. Current implementation only supports round-robin chunk mapping. Chunks
with the same ID from different tables must appear on the same node, to achieve
this chunk mapping is stored in CSS and reused (and updated) for tables loaded
at later time.

Data for non-partitioned tables in *multi-node* setup are loaded into every
worker database.


Generating Meta-data
====================

In addition to loading regular table data (chunked or non-chunked) the scripts
adds some additional data needed by Qserv.


Secondary Index
---------------

Secondary index is a special index in czar which provides mapping between object
ID for director table and its corresponding chunk and sub-chunk IDs. It is
currently implemented as a table in a special czar database indexed by object
ID. This table is created by data loader when director table data is loaded.
The name of the database is determined by ``--index-db`` command line option
and it can be set to empty string to prevent index generation. Index is never
made for non-director tables.


Empty Chunk List
----------------

This is the list of chunks that do not have any data, used by qserv for
optimization. This is stored as a file in the file system, its location is
determined by ``--empty-chunks`` option, default is not to produce the list.


Updating CSS Information
========================

Unless ``--no-css`` option is specified the script reads and updates CSS
information:

* if database-level partitioning parameters are not yet defined in CSS the
  script will store per-database parameters that it reads from configuration
  files, otherwise it will verify parameters read from configuration files
  against CSS parameters
* if ``--css-remove`` option is specified then any existing per-table CSS
  information will be removed from CSS, otherwise CSS must not have per-table
  data defined for this table
* it will create all necessary per-table parameters in CSS
* for partitioned tables it will read exiting mapping (if any) of the chunks
  to worker nodes, update it if there are new chunks, and store per-table chunk
  list after loading all chunks


Examples
========

With a large number of options and different running modes it's easy to get
overwhelmed or misinterpret loader errors. Here are few standard use cases
which are supposed to illustrate use of the command line options.


Mono-node setup, non-partitioned table
--------------------------------------

Simple use case when we load data for non-partitioned table. In *mono-node*
setup there are no worker databases, add data is loaded into one server, we
just need to provide correct connection options. Non-partitioned database
typically do not need per-table configuration file so there is just one
``common.cfg`` config file. Input data file is compressed so we will need
temporary location for uncompressed files, this is why ``--tmp-dir`` is
specified (select more unique name for it).

.. code-block:: bash

   TESTDATA=~/testdata-repo/datasets/case01/data
   db_options="--socket=$QRUNDIR/var/lib/mysql/mysql.sock --user=qsmaster"
   
   qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --tmp-dir=/tmp/data-loader-tmp \
       qservTest_case01_qserv LeapSeconds $TESTDATA/LeapSeconds.schema $TESTDATA/LeapSeconds.tsv.gz


Mono-node setup, partitioned table
----------------------------------

For partitioned tables there should be one additional per-table configuration
file which specifies table parameters. If non-default directory is used for
chunks then specify it with ``--chunks-dir`` options.

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --tmp-dir=/tmp/data-loader-tmp --chunks-dir=/tmp/data-loader-chunks \
        qservTest_case01_qserv Object $TESTDATA/Object.schema $TESTDATA/Object.tsv.gz


Mono-node setup, pre-partitioned data
-------------------------------------

In case when pre-partitioned data already exists one needs to provide its
location via ``--chunks-dir`` option and to tell script not to run partitioning
via ``--skip-partition`` option. No input file is needed in this case as the
data will be taken from chunks directory.

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --chunks-dir=/tmp/data-loader-chunks --skip-partition \
        qservTest_case01_qserv Object $TESTDATA/Object.schema


One-table setup
---------------

*One-table* mode is always triggered by ``--one--table`` option.

*One-table* option with non-partitioned table is not different from *mono-node*
option which also loads data into single table, there is no real need to
specify ``--one-table`` option in this case.

For partitioned tables there are two possible options for loading data - with or
without partitioning it. In most cases partitioning is not necessary, but it
may be needed in cases when partitioner is configured to do some non-trivial
operation on data (e.g. column dropping or re-ordering).

To load data without partitioning use ``--skip-partition`` together with
``--one--table``:

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --tmp-dir=/tmp/data-loader-tmp --one-table --skip-partition \
        qservTest_case01_mysql Object $TESTDATA/Object.schema $TESTDATA/Object.tsv.gz

To load data after partitioning use only ``--one--table``, ``--chunks-dir`` is
useful too in this case:

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --chunks-dir=/tmp/data-loader-chunks --tmp-dir=/tmp/data-loader-tmp --one-table \
        qservTest_case01_mysql Object $TESTDATA/Object.schema $TESTDATA/Object.tsv.gz


One-table setup, pre-partitioned data
-------------------------------------

In case there is data already pre-partitioned (e.g. from duplicator run) there
are two options for loading these data - either using script logic to find
chunks in chunk directory or manually selecting all chunk files (but not
overlap files) and passing it as an input to the script.

For first option specify ``--chunks-dir`` option but skip input files (and use
``--one-table`` and ``--skip-partition``):

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --chunks-dir=/tmp/data-loader-chunks --one-table --skip-partition \
        qservTest_case01_mysql Object $TESTDATA/Object.schema

second option is to specify all chunk files on the command line just like when
loading regular data into one table. It is important to avoid loading overlap
data in this case, choose file matching pattern accordingly. Because chunks are
not compressed there is no need to specify ``--tmp-dir`` option in this case:

.. code-block:: bash 

    qserv-data-loader.py $db_options --config=$TESTDATA/common.cfg --config=$TESTDATA/Object.cfg \
        --one-table --skip-partition \
        qservTest_case01_mysql Object $TESTDATA/Object.schema /tmp/data-loader-chunks/chunk_????.txt


Summary of Options
==================

Here is a summary table of all possible option combinations from above use cases
and their description, this only applies to partitioned tables:

+-----------+----------------+-------------+---------------------------------------------------------------+
| one-table | skip-partition | input files | description                                                   |
+===========+================+=============+===============================================================+
|   no      |     no         |     yes     | Partitions input files and loads into chunked tables          |
+-----------+----------------+-------------+---------------------------------------------------------------+
|   no      |     yes        |    ignored  | Loads pre-partitioned data from chunks-dir into chunked tables|
+-----------+----------------+-------------+---------------------------------------------------------------+
|   yes     |     no         |     yes     | Partitions input files and loads into one table               |
+-----------+----------------+-------------+---------------------------------------------------------------+
|   yes     |     yes        |     yes     | Loads input files into one table without partitioning         |
+-----------+----------------+-------------+---------------------------------------------------------------+
|   yes     |     yes        |     no      | Loads pre-partitioned data from chunks-dir into one table     |
+-----------+----------------+-------------+---------------------------------------------------------------+

