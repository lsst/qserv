.. _ingest-api-concepts-table-types:

Types of tables in Qserv
========================

.. note:

    Consider moving this section to the general documentatin on Qserv and refer to it from where
    it's needed.

Explain the tables and drow diagrams for:

- regular (fully replicated) tables
- partitioned tables, including sub-types

  - director tables
  - dependent tables

    - simple (1 director)
    - ref-match (2 directors)

Design of the partitioned tables. Draw a diagram that includes workers and chunk.


Draw a diagram of the table type classification and dependencies between them

- regular (fully-replicated)
  A copy of the table exists at each worker

- partitioned (distributed)
    - director
    - dependent
        - simple
        - ref-match
