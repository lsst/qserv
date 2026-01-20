.. _qserv-api-repl-registry:

Replication System's Registry API
=================================

The Replication system's registry API offers services for accessing and managing the locations
of various services within Qserv deployments. The ability to dynamically discover and track
services and their locations enables flexible interaction between Qserv components. The Registry
serves as a critical component of Qserv's distributed architecture.

Each registered service maintains the following information:

- **Service type**: Represents the service's role within Qserv (e.g., "controller", "czar"). Self-reported by the service.
- **Service name**: A unique identifier within the service type. Self-reported by the service.
- **Host name**: The FQDN of the machine running the service. Self-reported by the service.
- **Authoritative IP address**: The IPv4 or IPv6 address resolved by the Registry via DNS lookup of the reported host name.
- **Connection IP address**: The IP address observed from the incoming connection when the service reports its location.
  May differ from the authoritative IP address if the service operates behind a NAT or proxy.
- **Port numbers**: One or more ports on which the service listens for incoming connections.
- **Attributes**: Optional key-value pairs providing additional service metadata. Self-reported by the service.

The information is packaged into the JSON format.

Services periodically report their locations and metadata to the registry via the REST API. 
The registry records the timestamp of each service's last report, which is made available 
to registry clients alongside service location data.

.. note::

    Registry clients are responsible for determining service liveness and availability 
    based on the last report timestamp.

Service Types
-------------

The registry manages information for the following service types:

- **Replication Controller**: Manages replication tasks and coordinates activities of other replication system components.
- **Replication Workers**: Perform replication tasks as directed by the Replication Controller.
- **Qserv Czars**: Frontend services that provide client access to the Qserv system.
- **Qserv Workers**: Manage individual Qserv database instances.

General requirements of the API
-------------------------------

All REST API services of the Replication Registry require the following query parameter:

- ``instance_id``: A string specifying the identifier of a Qserv instance affected by the operation.
  A value of the parameter is used for the sanity checking to ensure requests are made to the correct instance of Qserv.

All ``POST``, ``PUT``, and ``DELETE`` requests are expected to contain a JSON object in the body.
The object must contain the following mandatory field:

- ``auth_key``: An authentication key required to authorize the operation.

The usual response object returned by all services upon completion of the operation has the following schema:

.. code-block::

    {
        "error"     : <string>,
        "error_ext" : <json>,
        "success"   : <number>,
        "warning"   : <string>
    }

Where:

- ``error``: A string describing the error if the request failed; empty string otherwise.
- ``error_ext``: A JSON object providing additional error details if the request failed; empty object otherwise.
- ``success``: An integer indicating the success status of the request (1 for success, 0 for failure).
- ``warning``: A string containing any warnings related to the request; empty string if none.


Retreiving locations of the services
------------------------------------

The following REST API service allows clients to obtain locations of all services know to the Registry:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``GET``
      - ``/services``

In case of the successful completion of the request, the service will return the following JSON object:

.. code-block::

    {
        "services"  : {
            "controllers" : {
                "master" : {
                    "host-name"      : <string>,
                    "host-addr"      : <string>,
                    "host-addr-peer" : <string>,
                    "id"             : <string>,
                    "port"           : <16-bit integer>,
                    "update-time-ms" : <64-bit integer>
                }
            },
            "czars" : {
                "http" : {
                    "host-name"       : <string>,
                    "host-addr"       : <string>,
                    "host-addr-peer"  : <string>,
                    "id"              : <string>,
                    "management-port" : <16-bit integer>,
                    "update-time-ms"  : <64-bit integer>
                },
                "proxy" : {
                    "host-name"       : <string>,
                    "host-addr"       : <string>,
                    "host-addr-peer"  : <string>,
                    "id"              : <string>,
                    "management-port" : <16-bit integer>,
                    "update-time-ms"  : <64-bit integer>
                }
            },
            "workers" : {
                <worker-id> : {
                    "qserv" : {
                        "host-name"       : <string>,
                        "host-addr"       : <string>,
                        "host-addr-peer"  : <string>,
                        "management-port" : <16-bit integer>,
                        "update-time-ms"  : <64-bit integer>
                    },
                    "replication" : {
                        "host-name"           : <string>,
                        "host-addr"           : <string>,
                        "host-addr-peer"      : <string>,
                        "svc-port"            : <16-bit integer>,
                        "fs-port"             : <16-bit integer>,
                        "loader-port"         : <16-bit integer>,
                        "exporter-port"       : <16-bit integer>,
                        "http-loader-port"    : <16-bit integer>,
                        "data-dir"            : <string>,
                        "exporter-tmp-dir"    : <string>,
                        "http-loader-tmp-dir" : <string>,
                        "loader-tmp-dir"      : <string>,
                        "update-time-ms"      : <64-bit integer>
                    }
                },
                ...
            }
        }
    }

Where:

- ``services``: A JSON object containing the locations and metadata of all registered services, organized by service type and name.
- ``host-name``: The FQDN of the machine running the service.
- ``host-addr``: The authoritative IP address of the service.
- ``host-addr-peer``: The IP address observed from the incoming connection when the service reported its location. Normally, it should
  be the same as ``host-addr`` unless the service is behind a NAT or proxy.
- ``id``: The unique identifier of the service within its type.
- ``port``: The port number on which the Replication Controller service listens for incoming connections.
- ``management-port``: The management port number for Qserv Czar and worker services.
- ``svc-port``: The service port number for Replication Worker services.
- ``fs-port``: The file system port number for Replication Worker services. The port is used by the Replication worker's file system services to communicate with other Replication Workers.
- ``loader-port``: The loader port number for Replication Worker services.
- ``exporter-port``: The exporter port number for Replication Worker services.
- ``http-loader-port``: The HTTP loader port number for Replication Worker services.
- ``data-dir``: The MySQL data directory path. The directory is used by Replication Worker services to directly access and manage the MySQL data files of the Qserv worker instance.
- ``exporter-tmp-dir``: The temporary directory path for the exporter component of Replication Worker services.
- ``http-loader-tmp-dir``: The temporary directory path for the HTTP loader component of Replication Worker services.
- ``loader-tmp-dir``: The temporary directory path for the loader component of Replication Worker services.
- ``update-time-ms``: A timestamp (in milliseconds since the epoch) indicating when the service last reported its location to the registry.

An example illustrating using the service:

.. code-block:: bash

    curl -X GET http://sdfqserv001.sdf.slac.stanford.edu:25882/services?instance_id=qserv6test

A successful response is shown below:

.. code-block:: json

    {
        "error"     : "",
        "error_ext" : {},
        "success"   : 1,
        "warning"   : "No version number was provided in the request."
        "services" : {
            "controllers" : {
                "master" : {
                    "host-name"      : "sdfqserv001.sdf.slac.stanford.edu",
                    "host-addr"      : "172.24.49.51",
                    "host-addr-peer" : "172.24.49.51",
                    "id"             : "d16d5155-3f60-4e2c-8c5f-907d4b12b365",
                    "port"           : 25881,
                    "update-time-ms" : 1768940398202
                }
            },
            "czars" : {
                "http" : {
                    "host-name"       : "sdfqserv001.sdf.slac.stanford.edu",
                    "host-addr"       : "172.24.49.51",
                    "host-addr-peer"  : "172.24.49.51",
                    "id"              : 8,
                    "management-port" : 37839,
                    "update-time-ms"  : 1768940399701
                },
                "proxy" : {
                    "host-name"       : "sdfqserv001.sdf.slac.stanford.edu",
                    "host-addr"       : "172.24.49.51",
                    "host-addr-peer"  : "172.24.49.51",
                    "id"              : 9,
                    "management-port" : 39983,
                    "update-time-ms"  : 1768940399576
                }
            },
            "workers" : {
                "db02" : {
                    "qserv" : {
                        "host-name"       : "sdfqserv002.sdf.slac.stanford.edu",
                        "host-addr"       : "172.24.49.52",
                        "host-addr-peer"  : "172.24.49.52",
                        "management-port" : 43479,
                        "update-time-ms"  : 1768940399701
                    },
                    "replication" : {
                        "host-name"           : "sdfqserv002.sdf.slac.stanford.edu",
                        "host-addr"           : "172.24.49.52",
                        "host-addr-peer"      : "172.24.49.52",
                        "svc-port"            : 25800,
                        "fs-port"             : 25801,
                        "loader-port"         : 25802,
                        "exporter-port"       : 25803,
                        "http-loader-port"    : 25804,
                        "data-dir"            : "/qserv/data/mysql",
                        "exporter-tmp-dir"    : "/qserv/data/export",
                        "http-loader-tmp-dir" : "/qserv/data/ingest",
                        "loader-tmp-dir"      : "/qserv/data/ingest",
                        "update-time-ms"      : 1768940395177
                    }
                }
            }
        }

Replication Controllers
-----------------------

Adding/updating
^^^^^^^^^^^^^^^

The following REST API service registers (or updates a previous entry on) the Replication Controllers:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``POST``
      - ``/controller``

The service updates the Registry for one Controller that is described in the body of the request. The schema of
the mandatory JSON object in the body is as follows:

.. code-block::
    
    {
        "controller" : {
            "host-name" : <string>,
            "name"      : <string>,
            ...
        }
    }

Where, the mandatory fields are:

- ``host-name``: The FQDN of the machine running the service.
- ``name``: The name of the Controller. Presently, the only valid value is "master". Other controllers may be supported in the future
  to support failover and high-availability configurations.

The object may contain any additional fields. including Controiller's unique ``id``, its service ``port``, and other metadata.

Removing
^^^^^^^^

The following REST API service removes a previously registered Replication Controller from the Registry:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``DELETE``
      - ``/controller/:name``

Where the mandatory path parameter ``name`` specifies the name of the Controller to be removed.


Qserv czars
-----------

Adding/updating 
^^^^^^^^^^^^^^^

The following REST API service registers (or updates a previous entry on) a Qserv Czar:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``POST``
      - ``/czar``

The service updates the Registry for one Czar that is described in the body of the request. The schema of
the mandatory JSON object in the body is as follows:

.. code-block::
    
    {
        "czar" : {
            "host-name" : <string>,
            "name"      : <string>,
            ...
        }
    }

Where, the mandatory fields are:

- ``host-name``: The FQDN of the machine running the service.
- ``name``: The name of the Czar. Presently, the only valid values are "http" and "proxy". Other czars may be supported in the future
  to support additional functionalities.

The object may contain any additional fields. including Czar's unique ``id``, its ``management-port``, and other metadata.

Removing
^^^^^^^^

The following REST API service removes a previously registered Qserv Czar from the Registry:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``DELETE``
      - ``/czar/:name``

Where the mandatory path parameter ``name`` specifies the name of the Czar to be removed.

Workers
-------

Adding/updating Qserv workers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


The following REST API service registers (or updates a previous entry on) a Qserv Worker:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``POST``
      - ``/qserv-worker``

The service updates the Registry for one Qserv Worker that is described in the body of the request. The schema of
the mandatory JSON object in the body is as follows:

.. code-block::
    
    {
        "worker" : {
            "host-name" : <string>,
            "name"      : <string>,
            ...
        }
    }

Where, the mandatory fields are:

- ``host-name``: The FQDN of the machine running the service.
- ``name``: The unique identifier of the Worker. It must correspond to the identifier of the corresponding worker Qserv database instance managed by the Worker.

The object may contain any additional fields. including Worker's unique ``id``, its ``management-port``, and other metadata.

Adding/updating Replication system's workers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following REST API service registers (or updates a previous entry on) a Replication Worker:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``POST``
      - ``/worker``

The service updates the Registry for one Replication Worker that is described in the body of the request. The schema of
the mandatory JSON object in the body is as follows:

.. code-block::
    
    {
        "worker" : {
            "host-name" : <string>,
            "name"      : <string>,
            ...
        }
    }

Where, the mandatory fields are:

- ``host-name``: The FQDN of the machine running the service.
- ``name``: The unique identifier of the Worker. It must correspond to the identifier of the corresponding worker Qserv database instance managed by the Worker.

The object may contain any additional fields. including Worker's unique ``id``, its ``management-port``, and other metadata.

Removing workers
^^^^^^^^^^^^^^^^

The following REST API service removes a previously registered Qserv and Replication Worker from the Registry:

..  list-table::
    :widths: 10 90
    :header-rows: 1

    * - method
      - service
    * - ``DELETE``
      - ``/worker/:name``

Where the mandatory path parameter ``name`` specifies the name of the Worker to be removed.
