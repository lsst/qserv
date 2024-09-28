########################
Set up ingest input data
########################

Prerequisites
=============

- An HTTP(s) server providing access to input data and metadata.

Example
=======

These `examples for input data <https://github.com/lsst-dm/qserv-ingest/tree/main/itest/datasets>`_
are used by `Qserv ingest continuous integration process <https://github.com/lsst-dm/qserv-ingest/actions>`_.


Input data
==========

Input data is produced by `Qserv partitioner <https://github.com/lsst/partition>`_ (i.e. ``sph-partition``) and is made of multiples ``*.csv``, ``*.tsv`` or ``*.txt`` files.
Each of these files contains a part of a chunk for a given database and table,
as shown in `this example <https://github.com/lsst-dm/qserv-ingest/blob/main/itest/datasets/case01/partition/case01/Source/chunk_6630.txt>`_.
Relation between an input data file and its related table and database is available inside ``metadata.json``, detailed below.

.. _Metadata:

Metadata
========

Metadata files below describe input data and are required by ``qserv-ingest``:

- ``metadata.json``: contain the name of the configuration files describing the database, the tables, and the indexes.
  It also contains the relative path to the input chunk data produced by Qserv partitioner.
  Folder organization for input chunk files is configurable, using the ``directory`` and the ``chunks`` sections of ``metadata.json``.
  Each input chunk file name must follow the pattern ``chunk_<chunk_id>.txt``.
  Add ``.tables[i].data[i].overlaps`` section if, for a given chunk, a chunk contribution file does not have a corresponding overlap file, and vice-versa,
  this might happen if a chunk has an empty overlap or if an empty chunk has a non-empty overlap.
  If ``.tables[i].data[i].overlaps`` section is missing then, for a given chunk, each chunk contribution file must have a corresponding overlap file (i.e. chunk_XXX.txt and chunk_XXX_overlap.txt must exist).

.. code:: json

    {
    "version":12
    "database":"test101.json",
    "formats":{
        "txt":{
           "fields_terminated_by":","
           "fields_escaped_by":"\\\\"
           "lines_terminated_by":"\\n"
        }
     },
    "tables":[
        {
            "schema":"director_table.json",
            "indexes":[
                "idx_director.json"
            ],
            "data":[
                {
                "directory":"director/dir1",
                "chunks":[
                    57866,
                    57867
                ]
                "overlaps":[
                    57800,
                    57801
                ]
                },
                {
                "directory":"director/dir2",
                "chunks":[
                    57868
                ]
                }
            ]
        },
        {
            "schema":"partitioned_table.json",
            "indexes":[
                "idx_partitioned.json"
            ],
            "data":[
                {
                "directory":"partitioned/dir1",
                "chunks":[
                    57866,
                    57867
                ]
                },
                {
                "directory":"partitioned/dir2",
                "chunks":[
                    57868
                ]
                }
            ]
        }
        ]
    }

- ``<database_name>.json``: describe the database to register inside the replication service and where the data will be ingested
  `Ingest API documentation for registering databases <https://confluence.lsstcorp.org/display/DM/2.+Registering+databases>`_
- ``<table_name>.json``: each of these files describes a table to register inside the replication service and where the data will be ingested,
  `Ingest API documentation for registering tables <https://confluence.lsstcorp.org/display/DM/3.+Registering+tables>`_
- ``<table_index>.json``:each of these files describes an index to create for a given set of chunk tables,
  `Ingest API documentation for creating indexes <https://confluence.lsstcorp.org/display/DM/Managing+indexes+of+MySQL+tables+at+Qserv+workers#ManagingindexesofMySQLtablesatQservworkers-Creatinganewindex>`_

A valid set of examples for all of these files is available in this `Rubin IN2P3 repository <https://github.com/rubin-in2p3/qserv-ingest-schema>`_.
