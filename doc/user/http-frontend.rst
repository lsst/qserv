
.. note::

  - This guide corresponds to version **54** of the Qserv REST API. Note that each API implementation has a specific version.
    The version number will change if any modifications to the implementation or API that might affect users are made.
    This document will be updated to reflect the latest API version.
  - All communications with the service are over SSL/TLS encrypted connections.
    The service will not accept unencrypted connections. Use the ``-k`` option with ``curl`` to bypass SSL certificate
    verification if necessary.

.. _http-frontend:

######################
HTTP frontend of Qserv
######################

.. toctree::
   :maxdepth: 4

   http-frontend-general
   http-frontend-query
   http-frontend-ingest

This document describes the HTTP-based frontend for interacting with Qserv. This frontend complements
the one based on ``mysql-proxy``.

Key features of the API presented in this document include:

- Utilizes the HTTP protocol.
- Returns result sets (data and schema) in JSON objects.
- Supports both *synchronous* and *asynchronous* query submission operations.
- Offers a mechanism for tracking the progress of asynchronously submitted queries.
- Allows for query cancellation.
- Provides a simple interface for ingesting and managing user tables.
- Ensures protocol versioning to maintain the integrity of distributed applications.
