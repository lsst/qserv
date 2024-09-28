.. _ingest-data-partitioning-ref-match:

Partitioning data of the ref-match tables
=========================================

Introduction
------------

The *ref-match* tables are a special class of tables that are designed to match rows between two independent *director* tables
belonging to the same or different catalogs. In this case, there is no obvious 1-to-1 match between rows of the director tables.
Instead, the pipelines compute a (spatial) match table between the two that provides a many-to-many relationship between both tables.
The complication is that a match record might reference a row (an *object*) and reference row that fall on opposite sides of
the partition boundary (into different chunks). Qserv deals with this by taking advantage of the overlap that must be stored
alongside each partition (this overlap is stored so that Qserv can avoid inter-worker communication when performing
spatial joins on the fly).

Since the *ref-match* tables are also partitioned tables the input data (CSV) of the tables have to be partitioned into chunks.
In order to partition the *ref-match* tables one would have to use a special version of the partitioning tool sph-partition-matches.
A source code of the tool is found in the source tree of Qserv: https://github.com/lsst/qserv/blob/main/src/partition/sph-partition-matches.cc.
The corresponding binary is built and placed into the binary Docker image of Qserv.

Here is an example illustrating how to launch the tool from the container:

.. code-block:: bash

    % docker run -it qserv/lite-qserv:2022.9.1-rc1 sph-partition-matches --help

        sph-partition-matches [options]

        The match partitioner partitions one or more input CSV files in
        preparation for loading by database worker nodes. This involves assigning
        both positions in a match pair to a location in a 2-level subdivision
        scheme, where a location consists of a chunk and sub-chunk ID, and
        outputting the match pair once for each distinct location. Match pairs
        are bucket-sorted by chunk ID, resulting in chunk files that can then
        be distributed to worker nodes for loading.
        A partitioned data-set can be built-up incrementally by running the
        partitioner with disjoint input file sets and the same output directory.
        Beware - the output CSV format, partitioning parameters, and worker
        node count MUST be identical between runs. Additionally, only one
        partitioner process should write to a given output directory at a
        time. If any of these conditions are not met, then the resulting
        chunk files will be corrupt and/or useless.
        \_____________________ Common:
        -h [ --help ]                         Demystify program usage.
        -v [ --verbose ]                      Chatty output.
        -c [ --config-file ] arg              The name of a configuration file
                                              containing program option values in a
        ...

The tool has two parameters specifying the locations of the input (CSV) file and the output folder where
the partitioned products will be stored:

.. code-block:: bash

    % sph-partition-matches --help
    ..
    \_____________________ Output:
      --out.dir arg                         The directory to write output files to.
    \______________________ Input:
      -i [ --in.path ] arg                  An input file or directory name. If the
                                            name identifies a directory, then all
                                            the files and symbolic links to files
                                            in the directory are treated as inputs.
                                            This option must be specified at least
                                            once.

.. hint::

    If the tool is launched via the docker command as was shown above, one would have to mount the corresponding
    host paths into the container.

All tables, including both *director* tables and the *ref-match* table itself, have to be partitioned using
the same values of the partitioning parameters, including:

- The number of stripes
- The number of sub-stripes
- The overlap radius

Values of the partitioning parameters should be specified using the following options (the default values shown below are meaningless
for any production scenario):

.. code-block:: bash

    --part.num-stripes arg (=18)          The number of latitude angle stripes to
                                          divide the sky into.
    --part.num-sub-stripes arg (=100)     The number of sub-stripes to divide
                                          each stripe into.
    --part.overlap arg (=0.01)            Chunk/sub-chunk overlap radius (deg).

The next sections present two options for partitioning the input data.

The spatial match within the given overlap radius
-------------------------------------------------

This is the most reliable way of partitioning the input data of the match tables. It is available when
the input rows of the match table carry the exact spatial coordinates of both matched rows (from the corresponding
*director* tables).

In this scenario, the input data file (``CSV``) is expected to have 4 columns representing the spatial coordinates
of the matched rows from the *director* tables on the 1st ("left") and on the 2nd ("righ"). Roles and sample names
of the columns are presented in the table below:

``dir1_ra``
  The *right ascent* coordinate (*longitude*) of the 1st matched entity (from the 1st *director* table).
``dir1_dec``
  The *declination* coordinate (*latitude*) of the 1st matched entity (from the 1st director table).
``dir2_ra``
  The *right ascent* coordinate (*longitude*) of the 2nd matched entity (from the 2nd *director* table).
``dir2_dec``
  The *declination* coordinate (*latitude*) of the 2nd matched entity (from the 2nd director table).

The names of these columns need to be passed to the partitioning tool using two special parameters:

.. code-block:: bash

    % sph-partition-matches \
        --part.pos1="dir1_ra,dir1_dec" \
        --part.pos2="dir2_ra,dir2_dec"

.. note:

    The order of the columns in each packed pair pf columns is important. The names must be separated by commas.

When using this technique for partitioning the match tables, it's required that the input CSV file(s) had at least those 4 columns
mentioned above. The actual number of columns could be larger. Values of all additional will be copied into the partitioned
products (the chunk files). The original order of the columns will be preserved.

Here is an example of a sample ``CSV`` file that has values of the above-described spatial coordinates in the first 4 columns
and the object identifiers of the corresponding rows from the matched *director* tables in the last 2 columns:

.. code-block::

    10.101,43.021,10.101,43.021,123456,6788404040
    10.101,43.021,10.102,43.023,123456,6788404041

The last two columns are meant to store values of the following columns:

``dir1_objectId``
  The unique object identifier of the 1st *director* table.
``dir2_objectId``
  The unique object identifier of the 2nd *director* table.

The input CSV file shown above could be also presented in the tabular format:

..  list-table::
    :widths: 10 10 10 10 10 10
    :header-rows: 1

    * - ``dir1_ra``
      - ``dir1_dec``
      - ``dir2_ra``
      - ``dir2_dec``
      - ``dir1_objectId``
      - ``dir2_objectId``
    * - 0.101
      - 43.021
      - 10.101
      - 43.021
      - 123456
      - 6788404040
    * - 0.101
      - 43.021
      - 10.102
      - 43.023
      - 123456
      - 6788404041

Note that this is actually a 1-to-2 match, in which a single object (``123456``) of the 1st director has two matched
objects (``6788404040`` and ``6788404041``) in the 2nd director. Also, note that the second matched object has slightly
different spatial coordinates than the first one. If the value of the overlap parameter is bigger than the difference
between the coordinates then the tool will be able to match the objects successfully. For example, this would work if
a value of the overlap was set to ``0.01``. Otherwise, no match will be made and the row will be ignored by the tool.

.. _warning:

    It is assumed that the input data of the *ref-match* tables are correctly produced by the data processing
    pipelines. Verifying the quality of the input data is beyond the scope of this document. However, one might
    consider writing a special tool for pre-scanning the input files and finding problems in the files.

Here is the complete practical example of how to run the tool with the assumptions made above:

.. code-block:: bash

    % cat in.csv
    10.101,43.021,10.101,43.021,123456,6788404040
    10.101,43.021,10.102,43.023,123456,6788404041

    % cat config.json
    {
        "part":{
            "num-stripes":340.
            "num-sub-stripes":3,
            "overlap":0.01,
            "pos1":"dir1_ra,dir1_dec",
            "pos2":"dir2_ra,dir2_dec"
        },
        "in":{
            "csv":{
                "null":"\\N",
                "delimiter":",",
                "field":[
                    "dir1_ra",
                    "dir1_dec"
                    "dir2_ra",
                    "dir2_dec",
                    "dir1_objectId",
                    "dir2_objectId"
                ]
            }
        },
        "out":{
            "csv":{
                "null":"\\N",
                "delimiter":",",
                "escape":"\\",
                "no-quote":true
            }
        }
    }

    % mkdir chunks
    % sph-partition-matches -c config.json --in.path=in.csv --out.dir=chunks/

Partitioning using index maps
-----------------------------

.. note::

    This section is under construction. Only the basic idea is presented here.

This is an alternative way of partitioning the input data of the match tables. It is available when the input rows of the match table
do not carry the exact spatial coordinates of both matched rows (from the corresponding *director* tables). Instead, the input data
has to carry the unique object identifiers of the matched rows. The tool will use the object identifiers to find the spatial coordinates
of the matched rows in the *director* tables. The tool will use the index maps of the *director* tables to find the spatial coordinates
of the matched rows.
