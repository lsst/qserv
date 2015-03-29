
This package contains code for the Qserv Worker Management Service (a.k.a.
wmgr). Worker Management Service is going to be the main and probably the
only communication endpoint for interacting with all worker services
(potentially similar service may be used to communicate with czar services).
For main ideas behind wmgr together and high-level design please see
[Trac page](https://dev.lsstcorp.org/trac/wiki/db/Qserv/WMGRDesign).

Wmgr is an HTTP-based RESTful service which interacts with other services
(mysql and xrootd). It is implemented currently as a standalone script
`qservWmgr.py` which is started automatically together with other
services when one runs `qserv-start.sh` script. `qservWmgr.py`
listens on a TCP port which is defined in `etc/qserv-wmgr.cnf` file.

There will be separate client library for communication with wmgr but for
quick tests it is possible to use plain HTTP to interact with wmgr. Here
are few quick examples of "calling" wmgr using `curl` (assuming that wmgr
listens on port 5012 on local host):

Get the list of databases:

    $ curl -i http://127.0.0.1:5012/dbs
    HTTP/1.0 200 OK
    Content-Type: application/json
    Content-Length: 839
    Server: Werkzeug/0.9.4 Python/2.7.5
    Date: Sun, 29 Mar 2015 04:44:17 GMT
    
    {"results": [{"name": "qservTest_case01_qserv", "uri": "/dbs/qservTest_case01_qserv"}, ...]}

Get the list of tables in a database:

    $ curl -i http://127.0.0.1:5012/dbs/qservTest_case01_qserv/tables
    HTTP/1.0 200 OK
    Content-Type: application/json
    Content-Length: 4515
    Server: Werkzeug/0.9.4 Python/2.7.5
    Date: Sun, 29 Mar 2015 04:51:15 GMT
    
    {"results": [{"name": "Filter", "uri": "/dbs/qservTest_case01_qserv/tables/Filter"}, ...]}


Get the list of services:

    $ curl -i http://127.0.0.1:5012/services
    HTTP/1.0 200 OK
    Content-Type: application/json
    Content-Length: 107
    Server: Werkzeug/0.9.4 Python/2.7.5
    Date: Sun, 29 Mar 2015 04:40:08 GMT
    
    {"results": [{"name": "xrootd", "uri": "/services/xrootd"}, {"name": "mysqld", "uri": "/services/mysqld"}]}

Get the status of mysqld service:

    $ curl -i http://127.0.0.1:5012/services/mysqld
    HTTP/1.0 200 OK
    Content-Type: application/json
    Content-Length: 77
    Server: Werkzeug/0.9.4 Python/2.7.5
    Date: Sun, 29 Mar 2015 04:41:16 GMT
    
    {"results": {"name": "mysqld", "state": "active", "uri": "/services/mysqld"}}
