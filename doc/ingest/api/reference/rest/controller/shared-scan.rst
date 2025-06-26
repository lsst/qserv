.. _ingest-shared-scan:

Configuration of the Shared Scans
=================================


.. _ingest-shared-scan-get:

Retrieving the Shared Scan Configuration
----------------------------------------

The service of the **Master Replication Controller** return information about the shared scan configuration
retreived from the Qserv's CSS service:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``GET``
      - ``/replication/qserv/css/shared-scan``

The response object is documented in:

- :ref:`ingest-shared-scan-result-schema`

.. _ingest-shared-scan-set:

Setting the Shared Scan Configuration
-------------------------------------

The service of the **Master Replication Controller** allows to modify information about the shared
scan configuration of a given table within the Qserv's CSS service:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``PUT``
      - ``/replication/qserv/css/shared-scan/:database-name/:table-name``

The request's URL must include the following parameters:

``database-name`` : *string*
  The placeholder for the  name of a database in the family.

``table-name`` : *string*
  The placeholder for the name of a table in the database.

The input JSON object sent in the request's *body* must have the following schema:

.. code-block::

    {
        "scanRating": <number>
    }

Where:

``scanRating`` : *number*
  The *scan rating* of the table in the database. It must be a positive number.
  The higher the value, the higher the "weight" of the table in the user queries. The weight also 
  translates into expectations for the amount of resources that need to be consumed for processing
  the table during the query execution. The value is used by the Qserv Czar to assign a priority
  to queries involving the table.

The response object will have the updated state of the Qserv's configuration. It's documented in:

- :ref:`ingest-shared-scan-result-schema`

.. _ingest-shared-scan-result-schema:

Schema of the result object
---------------------------

The schema is the same for both the GET and PUT methods:

.. code-block::

    {
        "css": {
            "shared_scan": {
                <family-name>: {
                    <database-name>: {
                        <table-name>: {
                            "scanRating": <number>
                        },
                        ...                        
                    },
                    ...
                },
                ...
            }
        }
    }

Where:

``family-name`` : *string*
  The name (a unique identifier) of a family the member database belong to.

``database-name`` : *string*
  The placeholder for the name of a database in the family.

``table-name`` : *string*
  The placeholder for the name of a table in the database.

``scanRating`` : *number*
  The *scan rating* of the table in the database. It is a positive number.
  The higher the value, the higher the "weight" of the table in the user queries. The weight also 
  translates into expectations for the amount of resources that need to be consumed for processing
  the table during the query execution. The value is used by the Qserv Czar to assign a priority
  to queries involving the table.
