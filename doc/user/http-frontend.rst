
.. note::

   - Information in this guide corresponds to the version **38** of the Qserv REST API. Keep in mind
     that each implementation of the API has a specific version. The version number will change
     if any changes to the implementation or the API that might affect users will be made.
     The current document will be kept updated to reflect the latest version of the API.
   - As of the version **38** all comuunicatons with the service are over the SSL/TLS encrypted
     connection. The service will not accept the unencrypted connections. Use option ``-k`` when
     using ``curl`` to bypass the SSL certificate verification if needed.

.. _http-frontend:

######################
HTTP frontend of Qserv
######################

.. toctree::
   :maxdepth: 4

   http-frontend-general
   http-frontend-query
   http-frontend-ingest

The document describes the HTTP-based frontend for interacting with Qserv. The frontend complements
the frontend based on ``mysql-proxy``.

Key features of the API presented in this document are:

- It's based on the HTTP protocol.
- Result sets (data and schema) are returned in the JSON objects.
- Both *synchronous* and *asynchronous* query submission operations are supported.
- It provides a mechanism for tracking the progress of the asynchronously submitted queries.
- It allows query cancellation.
- It supports a simple interface for ingesting and managing user tables
- It provides the protocol versioning to ensure the integrity of the distributed applications.
