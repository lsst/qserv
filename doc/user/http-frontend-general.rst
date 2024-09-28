
General guidelines
==================

This section provides an overview of the general guidelines for utilizing the REST services of the frontend.

JSON
---------

All services, regardless of the HTTP *method* (``GET``, ``POST``, etc.), report results in JSON objects.
The schema of these objects varies depending on the service called. Detailed explanations of
the schemas can be found in the corresponding sections where the services are described.

.. _http-frontend-general-error-reporting:

Error reporting
---------------

When a service receives a request, it will always return a response with the HTTP code ``200``.
The actual completion status of the request is indicated in the JSON object:

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

Application developers are encouraged to use the mechanism to reinforce the 
integrity of the applications.

Client applications can utilize version numbers in two ways:

- *Pull mode*: The client explicitly requests the version implemented by the frontend and compares it with the expected version.
- *Push mode*: The client includes the expected version number as a parameter in service requests, allowing the services to verify
  if the version matches the frontend implementation.

Developers can choose to use either method, both, or neither to ensure application integrity.

Pull mode
^^^^^^^^^

To support the first scenario, the API provides a special metadata service that returns
the version number along with additional information about the frontend:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/meta/version``

The request object for this service is not required, or it can be an empty JSON object ``{}``.
Upon successful completion, the service will return a JSON object containing the following attributes:

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
the following request for checking the status of an ongoing query might look like this:

.. code-block:: bash

   curl -k 'https://localhost:4041/query-async/status/1234?version=38' -X GET

For other HTTP methods used by the API, the version number must be included in the body of the request as shown below:

.. code-block:: bash

   curl -k 'https://localhost:4041/query-async' -X POST \
        -H 'Content-Type: application/json' \
        -d'{"version":38,"query":"SELECT ..."}'

If the number does not match expectations, such a request will fail and the service will return the following
response. Here is an example of what will happen if the wrong version number ``29`` is specified instead
of ``38`` (as specified in the example above):

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
