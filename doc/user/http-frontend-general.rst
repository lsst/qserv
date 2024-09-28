
General guidelines
==================

This section introduces the general guidelines for using the REST services of the frontend.

JSON
---------

All services regardless of the HTTP method (GET, POST, etc.) report results in the JSON objects.
The schema of the objects varies depending on a service called. A detailed explanation of
the schema can be found in the corresponding sections where the services are explained.

.. _Error reporting:

Error reporting
---------------

If a service received a request it would always return a response with the HTTP code ``200``.
The actual completion status of a request is returned in the JSON object:

.. code-block::

    {   "success" : <number>,
        "error" : <string>,
        "error_ext" : <object>,
        "warning" : <string>
    }

The following attributes are related to the completion status:

``success`` : *number*
 The flag indicating if the request succeeded:

  - ``0`` if a request failed (then see attributes ``warning``, ``error``, and ``error_ext``)
  - any other number if a request succeeded (also inspect the attribute ``warning`` for non-critical notifications)

``error`` : *string*
  An explanation of the error (if any).

``error_ext`` : *object*
  The optional information on the error (if any). The content of the object is request-specific.
  Details can be found in the reference section of the corresponding request.

``warning`` : *string*
  The optional warning for on-critical conditions. Note that warnings may be posted for both
  completed or failed requests.

Other HTTP codes (``3xx``, ``404``, ``5xx``, etc.) could also be returned by the frontend's HTTP server or intermediate proxy servers.

Protocol Versioning
-------------------

The API adheres to the optional version control mechanism introduced in:

- https://rubinobs.atlassian.net/browse/DM-35456 

Application developers are encouraged to use the mechanism to reinforce the integrity of the applications.

There are two ways the client applications can use the version numbers:

- *pull mode*: Ask the frontend explicitly what version it implements and cross-check the returned
  version versus a number expected by the application.
- *push mode*: Pass the expected version number as a parameter when calling the services and let
  the services verify if that version matches one of the frontend implementations.

Application developers are free to use neither, either of two, or both methods of reinforcing their applications.

Pull mode
^^^^^^^^^

To support the first scenario, the API provides a special metadata service that will return
the version number (along with some other information on the frontend):

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/meta/version``

The request object for this request is not required, or it could be an empty JSON object ``{}``.
In case of its successful completion, the service will return a JSON object that will include
the following attributes:

.. code-block::

    {   "kind" :        <string>,
        "name" :        <string>,
        "id" :          <number>,
        "instance_id" : <string>,
        "version" :     <number>
    }

Where:

``kind`` : *string*
  The name of the service. The current implementation always reports:

  .. code-block::

    qserv-czar-query-frontend

``name`` : *string*
  The unique name of the frontend within a given Qserv. The current implementation always reports:

  .. code-block::

    http

``id`` : *number*
  The numeric identifier of the frontend within a given Qserv. The number returned here may vary.

``instance_id`` : *string*
  An identifier of the Qserv. A value of the attribute depends on a particular deployment of Qserv.

``version`` : *number*
  The current version number of the API.

Example:

.. code-block:: json

    {   "kind" :        "qserv-czar-query-frontend",
        "name" :        "http",
        "id" :          8,
        "instance_id" : "qserv-prod",
        "version" :     38,
        "success" :     1
    }

Push mode
^^^^^^^^^

In the case of the second scenario, an application will pass the desired version number as
a request parameter. The number would be a part of the request's query for the method. For example,
the following request for checking the status of the ongoing query might look like this:

.. code-block:: bash

   curl -k 'https://localhost:4041/query-async/status/1234?version=38' -X GET

For other HTTP methods used by the API, the number is required to be provided within the body
of a request as shown below:

.. code-block:: bash

   curl -k 'https://localhost:4041/query-async' -X POST \
        -H 'Content-Type: application/json' \
        -d'{"version":38,"query":"SELECT ..."}'

If the number does not match expectations, such a request will fail and the service return the following
response. Here is an example of what will happen if the wrong version number ``29`` is specified instead
of ``38`` (as per the current version of the API):

.. code-block:: json

    {   "success" : 0,
        "error" :   "The requested version 29 of the API is not in the range
                     supported by the service.",
        "error_ext" : {
            "max_version" : 38,
            "min_version" : 30
        },
        "warning" : ""
    }
