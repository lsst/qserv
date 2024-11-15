.. _ingest-config:

Configuring parameters of the ingests
=====================================

.. _ingest-config-set:

Setting configuration parameters
--------------------------------

Parameters are set for a database (regardless of the *published* status) using the following service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``PUT``
      - ``/ingest/config``

The request object has the following schema:

.. code-block::

    {   "database" : <string>,
        "SSL_VERIFYHOST" : <number>,
        "SSL_VERIFYPEER" : <number>,
        "CAPATH" : <string>,
        "CAINFO" : <string>,
        "CAINFO_VAL" : <string>,
        "PROXY_SSL_VERIFYHOST" : <number>,
        "PROXY_SSL_VERIFYPEER" : <number>,
        "PROXY_CAPATH" : <string>,
        "PROXY_CAINFO" : <string>,
        "PROXY_CAINFO_VAL" : <string>,
        "CURLOPT_PROXY" : <string>,
        "CURLOPT_NOPROXY" : <string>,
        "CURLOPT_HTTPPROXYTUNNEL" : <number>,
        "CONNECTTIMEOUT" : <number>,
        "TIMEOUT" : <number>,
        "LOW_SPEED_LIMIT" : <number>,
        "LOW_SPEED_TIME" : <number>,
        "ASYNC_PROC_LIMIT" : <number>
    }

Where:

``database`` : *string* : **required**
  The required name of a database affected by the operation.

``SSL_VERIFYHOST`` : *number* = ``2``
  The optional flag that tells the system to verify the host of the peer. If the value is set
  to ``0`` the system will not check the host name against the certificate. Any other value would tell the system
  to perform the check.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_SSL_VERIFYHOST.html.

``SSL_VERIFYPEER`` : *number* = ``1``
  The optional flag that tells the system to verify the peer's certificate. If the value is set
  to ``0`` the system will not check the certificate. Any other value would tell the system to perform the check.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_SSL_VERIFYPEER.html.

``CAPATH`` : *string* = ``/etc/ssl/certs``
  The optional path to a directory holding multiple CA certificates. The system will use the certificates
  in the directory to verify the peer's certificate. If the value is set to an empty string the system will not use
  the certificates.

  Putting the empty string as a value of the parameter will effectively turn this option off as if it has never been
  configured for the database.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_CAPATH.html.

``CAINFO`` : *string* = ``/etc/ssl/certs/ca-certificates.crt``
  The optional path to a file holding a bundle of CA certificates. The system will use the certificates
  in the file to verify the peer's certificate. If the value is set to an empty string the system will not use
  the certificates.

  Putting the empty string as a value of the parameter will effectively turn this option off as if it has never been
  configured for the database.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_CAINFO.html.

``CAINFO_VAL`` : *string* = ``""``
  The optional value of a certificate bundle for a peer. This parameter is used in those cases when it's
  impossible to inject the bundle directly into the Ingest workers' environments. If a non-empty value of the parameter
  is provided then ingest servers will use it instead of the one mentioned (if any) in the above-described
  attribute ``CAINFO``.

  **Attention**: Values of the attribute are the actual certificates, not file paths like in the case of ``CAINFO``.

``PROXY_SSL_VERIFYHOST`` : *number* = ``2``
  The optional flag that tells the system to verify the host of the proxy. If the value is set
  to ``0`` the system will not check the host name against the certificate. Any other value would tell the system
  to perform the check.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_PROXY_SSL_VERIFYHOST.html.

``PROXY_SSL_VERIFYPEER`` : *number* = ``1``
  The optional flag that tells the system to verify the peer's certificate. If the value is set
  to ``0`` the system will not check the certificate. Any other value would tell the system to perform the check.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_PROXY_SSL_VERIFYPEER.html.

``PROXY_CAPATH`` : *string* = ``""``
  The optional path to a directory holding multiple CA certificates. The system will use the certificates
  in the directory to verify the peer's certificate. If the value is set to an empty string the system will not use
  the certificates.

  Putting the empty string as a value of the parameter will effectively turn this option off as if it has never been
  configured for the database.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_PROXY_CAPATH.html.

``PROXY_CAINFO`` : *string* = ``""``
  The optional path to a file holding a bundle of CA certificates. The system will use the certificates
  in the file to verify the peer's certificate. If the value is set to an empty string the system will not use
  the certificates.

  Putting the empty string as a value of the parameter will effectively turn this option off as if it has never been
  configured for the database.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_PROXY_CAINFO.html.

``PROXY_CAINFO_VAL`` : *string* = ``""``
  The optional value of a certificate bundle for a proxy. This parameter is used in those cases when it's
  impossible to inject the bundle directly into the Ingest workers' environments. If a non-empty value of the parameter
  is provided then ingest servers will use it instead of the one mentioned (if any) in the above-described
  attribute ``PROXY_CAINFO``.

  **Attention**: Values of the attribute are the actual certificates, not file paths like in the case of ``PROXY_CAINFO``.

``CURLOPT_PROXY`` : *string* = ``""``
  Set the optional proxy to use for the upcoming request. The parameter should be a null-terminated string
  holding the host name or dotted numerical IP address. A numerical IPv6 address must be written within ``[brackets]``.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_PROXY.html.

``CURLOPT_NOPROXY`` : *string* = ``""``
  The optional string consists of a comma-separated list of host names that do not require a proxy
  to get reached, even if one is specified.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_NOPROXY.html.

``CURLOPT_HTTPPROXYTUNNEL`` : *number* = ``0``
  Set the optional tunnel parameter to ``1`` to tunnel all operations through the HTTP proxy
  (set with ``CURLOPT_PROXY``).

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_HTTPPROXYTUNNEL.html.

``CONNECTTIMEOUT`` : *number* = ``0``
  The optional maximum time in seconds that the system will wait for a connection to be established.
  The default value means that the system will wait indefinitely.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_CONNECTTIMEOUT.html

``TIMEOUT`` : *number* = ``0``
  The optional maximum time in seconds that the system will wait for a response from the server.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_TIMEOUT.html

``LOW_SPEED_LIMIT`` : *number* = ``0``
  The optional transfer speed in bytes per second that the system considers too slow and will abort the transfer.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_LOW_SPEED_LIMIT.html

``LOW_SPEED_TIME`` : *number* = ``0``
  The optional time in seconds that the system will wait for the transfer speed to be above the limit
  set by ``LOW_SPEED_LIMIT``.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_LOW_SPEED_TIME.html

``ASYNC_PROC_LIMIT`` : *number* = ``0``
  The optional maximum concurrency limit for the number of contributions to be processed in a scope of
  the database. The actual number of parallel requests may be further lowered by the hard limit specified by
  the Replication System worker's configuration parameter (``worker``, ``num-async-loader-processing-threads``).
  The parameter can be adjusted in real time as needed. It gets into effect immediately. Putting ``0`` as a value of
  the parameter will effectively turn this option off as if it has never been configured for the database.

  This attribute directly maps to https://curl.se/libcurl/c/CURLOPT_LOW_SPEED_TIME.html

  **Note**: The parameter is available as of API version ``14``.

If a request is successfully finished it returns the standard JSON object w/o any additional data but
the standard completion status.

.. _ingest-config-get:

Retrieving configuration parameters
-----------------------------------

.. warning::
    As of version ``14`` of the API, the name of the database is required to be passed in the request's query instead of
    passing it in the JSON body. The older implementation was wrong.


..  list-table::
    :widths: 10 25 65
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/ingest/config``
      - ``database=<string>``

Where the mandatory query parameter ``database`` specifies the name of a database affected by the operation.

If the operation is successfully finished it returns an extended JSON object that has the following schema (in addition
to the standard status and error reporting attributes):

.. code-block::

    {   "database" : <string>,
        "SSL_VERIFYHOST" : <number>,
        "SSL_VERIFYPEER" : <number>,
        "CAPATH" : <string>,
        "CAINFO" : <string>,
        "CAINFO_VAL" : <string>,
        "PROXY_SSL_VERIFYHOST" : <number>,
        "PROXY_SSL_VERIFYPEER" : <number>,
        "PROXY_CAPATH" : <string>,
        "PROXY_CAINFO" : <string>,
        "PROXY_CAINFO_VAL" : <string>,
        "CURLOPT_PROXY" : <string>,
        "CURLOPT_NOPROXY" : <string>,
        "CURLOPT_HTTPPROXYTUNNEL" : <number>,
        "CONNECTTIMEOUT" : <number>,
        "TIMEOUT" : <number>,
        "LOW_SPEED_LIMIT" : <number>,
        "LOW_SPEED_TIME" : <number>,
        "ASYNC_PROC_LIMIT" : <number>
    }

The attributes of the response object are the same as the ones described in the section :ref:`ingest-config-set`.

.. _ingest-config-global-workers:

Global configuration parameters of workers
------------------------------------------

.. note::
    This is the same service that was described in:

    - :ref:`ingest-db-table-management-config` (REST)

    The response object of the service also returns the information on the workers.

There are two sectons related to workers in the response object. The first section ``config.general.worker``
includes the general parameters of the ingest services. Values of the parameters are the same for all
workers. The second section ``config.workers`` has the information on the individual workers.

The general information on all workers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema of the relevant section of the respionse object is illustrated by the following example:

.. code-block:: json

    {   "config": {
            "general" : {
                "worker" : {
                    "num-loader-processing-threads" : 64,
                    "num-http-loader-processing-threads" : 8,
                    "num-async-loader-processing-threads" : 8,

                    "ingest-charset-name" : "latin1",

                    "ingest-max-retries" : 10,
                    "ingest-num-retries" : 1,

                    "loader-max-warnings" : 64,

                    "async-loader-auto-resume" : 1,
                    "async-loader-cleanup-on-resume" : 1,
                },
            }
        }
    }

Where:

``config.general.worker`` : *object*
  A collection of the general parameters of the worker ingest service.

``num-loader-processing-threads`` : *number*
  The number of ingest request processing threads in the service that supports the proprietary
  binary protocol.

``num-http-loader-processing-threads`` : *number*
  The number of ingest request processing threads in the HTTP-based ingest service. Note that
  the service is used for processing *synchronous* contribution requess and for submitting
  the *asynchronous* requests to the service.

``num-async-loader-processing-threads`` : *number*
  The number of ingest request processing threads in a thread pool that processes
  the *asynchronous* contribution requests.

``ingest-charset-name`` : *string*
  The name of a character set for parsing the payload of the contributions.

``ingest-max-retries`` : *number*
  The maximum number of the automated retries of failed contribution attempts 
  in cases when such retries are still possible. The parameter represents the *hard*
  limit for the number of retries regardless of what's specified in the related
  parameter ``ingest-num-retries`` or in the contributions requests made by the workflows.
  The primary purpose of the parameter is to prevent accidental overloading
  of the ingest system should a very large number of retries accidentally specified
  by the ingest workflows for individual contributions. Setting a value of the parameter
  to ``0`` will unconditionally disable any retries.

``ingest-num-retries`` : *number*
  The default number of the automated retries of failed contribution attempts
  in cases when such retries are still possible. The limit can be changed for
  individual contributions. Note that the effective number of retries specified
  by this parameter or the one set in the contribution requests can not
  exceed the *hard* limit set in the related parameter ``ingest-max-retries``.
  Setting a value of the parameter to 0 will disable automatic retries (unless they are
  explicitly enabled or requested by the ingest workflows for individual contributions).

``loader-max-warnings`` : *number*
  The maximum number of warnings to retain after executing ``LOAD DATA [LOCAL] INFILE``
  when ingesting contributions into worker MySQL database. The warnings (if any) will be recorded in
  the persisent state of the Replication/Ingest system and returned to the ingest workflow upon request.

``async-loader-auto-resume`` : *number*
  The flag controlling the behavior of the worker's *asynchronous* ingest service after
  (the deliberate or accidental) restarts. If the value of the parameter is not ``0`` then the service
  will resume processing incomplete (queued or on-going) requests.  Setting a value of the parameter
  to ``0`` will result in the unconditional failing of all incomplete contribution requests existed prior
  the restart.
  
  .. warning::

    Requests failed in the last (loading) stage can't be resumed, and they will require aborting
    the corresponding transaction. If the automaticu resume is enabled rhese request will be automatically
    closed and marked as failed.

``async-loader-cleanup-on-resume`` : *number*
  The flag controlling the behavior of worker's *asynchronous* ingest service after
  restarting the service. If the value of the parameter is not ``0`` the service will try to clean
  up the temporary files that might be left on disk for incomplete (queued or ongoing) requests.
  The option may be disabled to allow debugging the service.

Worker-specific information
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema of the relevant section of the respionse object is illustrated by the following example:

.. code-block:: json

    {   "config": {
            "workers" : [
                {   "name" : "db02",
                    "is-enabled" : 1,
                    "is-read-only" : 0,

                    "loader-host" : {
                        "addr" : "172.24.49.52",
                        "name" : "sdfqserv002.sdf.slac.stanford.edu"
                    },
                    "loader-port" : 25002,
                    "loader-tmp-dir" : "/qserv/data/ingest",

                    "http-loader-host" : {
                        "name" : "sdfqserv002.sdf.slac.stanford.edu",
                        "addr" : "172.24.49.52"
                    },
                    "http-loader-port" : 25004,
                    "http-loader-tmp-dir" : "/qserv/data/ingest",
                },
            ]
        }
    }

Where:

``config.workers`` : *array*
  A collection of worker nodes, where each object represents a worker node.

``name`` : *string*
  The unique identifier of a worker node.

``is-enabled`` : *number*
  The flag that tells if the worker node is enabled. If the value is set to ``0`` the worker node is disabled.
  Workers which are not enables do not participate in the ingest activities.

``is-read-only`` : *number*
  The flag that tells if the worker node is read-only. If the value is set to ``0`` the worker node is read-write.
  Workers which are in the read-only statte do not participate in the ingest activities.

**Parameters of the ingest service that supports the proprietary binary protocol**:

``loader-host`` : *object*
  The object with the information about the loader host.

  - ``addr`` : *string*
    The IP address of the lder host.

  - ``name`` : *string*
    The FQDN (fully-qualified domain name) of the host.

``loader-port`` : *number*
  The port number of the ingest service.

``loader-tmp-dir`` : *string*
  The path to the temporary directory on the loader host that is used by the ingest service
  as a staging area for the contributions.

**Parameters of the HTTP-based ingest service**:

``http-loader-host`` : *object*
  The object with the information about the loader host.

  - ``addr`` : *string*
    The IP address of the lder host.

  - ``name`` : *string*
    The FQDN (fully-qualified domain name) of the host.

``http-loader-port`` : *number*
  The port number of the ingest service.

``http-loader-tmp-dir`` : *string*
  The path to the temporary directory on the loader host that is used by the ingest service
  as a staging area for the contributions.
