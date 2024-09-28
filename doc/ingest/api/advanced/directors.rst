
.. _ingest-api-advanced-directors:

Databases with many director tables
===================================

.. tip::

  See the following document first:

  - :ref:`ingest-api-concepts-table-types` (CONCEPTS)

The API supports ingesting multiple *director* tables within a single catalog. Each *director* table can optionally have its
own set of *dependent* tables. This section demonstrates the necessary configuration for the following table set:

.. table::

  +----------+-----------+
  | director | dependent |
  +==========+===========+
  | dir_1    | dep_1_1   |
  |          +-----------+
  |          | dep_1_2   |
  |          +-----------+
  |          | dep_1_3   |
  +----------+-----------+
  | dir_2    | dep_2_1   |
  |          +-----------+
  |          | dep_2_2   |
  +----------+-----------+
  | dir_3    |           |
  +----------+-----------+

In this example, there are 3 *director* tables. Each (except the third one ``dir_3``) has its own set of *dependent* tables.
The key attributes that govern dependencies between the tables are specified in the section :ref:`ingest-db-table-management-register-table`
(REST). The document mentions the following required attributes that need to be provided in the JSON specification of
each table:

.. table::

  +--------------------+----------+-----------------------------------------------------------------------------------------------------+
  | attribute          | value    | comment                                                                                             |
  +====================+==========+=====================================================================================================+
  | ``is_partitioned`` | ``1``    | Same value for both *director* and *dependent* tables.                                              |
  +--------------------+----------+-----------------------------------------------------------------------------------------------------+
  | ``director_table`` | *string* | Where:                                                                                              |
  |                    |          |                                                                                                     | 
  |                    |          | - *director* tables should have the empty string here.                                              |
  |                    |          | - *dependent* tables should have the name of the corresponding *director* table here.               |
  +--------------------+----------+-----------------------------------------------------------------------------------------------------+
  | ``director_key``   | *string* | The non-empty string specifying the name of the corresponding column must be                        |
  |                    |          | provided here. Depending on the type of the table, this name corresponds to either:                 |
  |                    |          |                                                                                                     | 
  |                    |          | - The *primary key* in the *director* table, or                                                     |
  |                    |          | - The *foreign key* pointing to the corresponding director's primary key in the *dependent* tables. |
  +--------------------+----------+-----------------------------------------------------------------------------------------------------+

The *director* tables are **required**  to have the following attributes:

.. table::

  +-------------------+----------+---------------------------------------------------------------------------+
  | attribute         | value    | comment                                                                   |
  +===================+==========+===========================================================================+
  | ``latitude_key``  | *string* | The names of the director table's columns that were used for partitioning |
  | ``longitude_key`` |          | the table data into chunks.                                               |
  +-------------------+----------+---------------------------------------------------------------------------+

The *dependent* tables may also include the attributes ``latitude_key`` and ``longitude_key`` if their input data was partitioned using the columns specified
by these attributes. If not, these attributes can be omitted from the table's JSON specification.

The following table illustrates how JSON configurations for all the above-mentioned tables might look like
(the examples were simplified for clarity):

.. table::

  +-----------------------------------+-------------------------------------------+
  | director                          | dependents                                |
  +===================================+===========================================+
  | .. code-block:: json              | .. code-block:: json                      |
  |                                   |                                           |
  |   { "table" : "dir_1",            |   { "table" : "dep_1_1",                  |
  |     "is_partitioned" : 1,         |     "is_partitioned" : 1,                 |
  |     "director_table" : "",        |     "director_table" : "dir_1",           |
  |     "director_key" : "objectId",  |     "director_key" : "dep_objectId"       |
  |     "latitude_key" : "ra",        |   }                                       |
  |     "longitude_key" : "dec"       |                                           |
  |   }                               | **Note**: Attributes ``latitude_key`` and |
  |                                   | ``longitude_key`` were not provided.      |
  |                                   | is allowed for the dependent tables.      |
  |                                   |                                           |
  |                                   | .. code-block:: json                      |
  |                                   |                                           |
  |                                   |   { "table" : "dep_1_2",                  |
  |                                   |     "is_partitioned" : 1,                 |
  |                                   |     "director_table" : "dir_1",           |
  |                                   |     "director_key" : "dep_objectId"       |
  |                                   |     "latitude_key" : "",                  |
  |                                   |     "longitude_key" : ""                  |
  |                                   |   }                                       |
  |                                   |                                           |
  |                                   | **Note**: Attributes ``latitude_key`` and |
  |                                   | ``longitude_key`` were provided. However  |
  |                                   | the values were empty strings, which is   |
  |                                   | allowed for the dependent tables.         |
  |                                   |                                           |
  |                                   | .. code-block:: json                      |
  |                                   |                                           |
  |                                   |   { "table" : "dep_1_3",                  |
  |                                   |     "is_partitioned" : 1,                 |
  |                                   |     "director_table" : "dir_1",           |
  |                                   |     "director_key" : "dep_objectId"       |
  |                                   |     "latitude_key" : "dep_ra",            |
  |                                   |     "longitude_key" : "dep_dec"           |
  |                                   |   }                                       |
  +-----------------------------------+-------------------------------------------+
  | .. code-block:: json              | .. code-block:: json                      |
  |                                   |                                           |
  |   { "table" : "dir_2",            |   { "table" : "dep_2_1",                  |
  |     "is_partitioned" : 1,         |     "is_partitioned" : 1,                 |
  |     "director_table" : "",        |     "director_table" : "dir_2",           |
  |     "director_key" : "id",        |     "director_key" : "dep_id"             |
  |     "latitude_key" : "coord_ra",  |   }                                       |
  |     "longitude_key" : "coord_dec" |                                           |
  |   }                               | .. code-block:: json                      |
  |                                   |                                           |
  |                                   |   { "table" : "dep_2_1",                  |
  |                                   |     "is_partitioned" : 1,                 |
  |                                   |     "director_table" : "dir_2",           |
  |                                   |     "director_key" : "dep_id"             |
  |                                   |     "latitude_key" : "dep_coord_ra",      |
  |                                   |     "longitude_key" : "dep_coord_dec"     |
  |                                   |   }                                       |
  +-----------------------------------+-------------------------------------------+
  | .. code-block:: json              |  No dependents for the *director* table   |
  |                                   |                                           |
  |   { "table" : "dir_3",            |                                           |
  |     "is_partitioned" : 1,         |                                           |
  |     "director_table" : "",        |                                           |
  |     "director_key" : "objectId",  |                                           |
  |     "latitude_key" : "ra",        |                                           |
  |     "longitude_key" : "dec"       |                                           |
  |   }                               |                                           |
  +-----------------------------------+-------------------------------------------+

.. note::

  The attributes ``chunk_id_key`` and ``sub_chunk_id_key`` were required in older versions of the API and may still
  be present in JSON configurations. However, they are no longer needed for registering tables during ingest.
  The role-to-column mapping for these attributes is now predefined in the Ingest system implementation.
  The mapping is presented below:

  +----------------------+----------------+
  | role                 | column         |
  +======================+================+
  | ``chunk_id_key``     | ``chunkId``    |
  +----------------------+----------------+
  | ``sub_chunk_id_key`` | ``subChunkId`` |
  +----------------------+----------------+

  If any of these attributes are found in a configuration, their definitions will be ignored.

