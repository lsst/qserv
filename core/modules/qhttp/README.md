# qhttp

This is a somewhat bare-bones C++ HTTP server, inspired by things like Express (http://expressjs.com/) and 
Martini (https://github.com/go-martini/martini).

The intended use-case is embedding a light-duty HTTP server within C++ back-end services, to conveniently
provide utility services (ReST APIs, served status pages, small browser applications, etc.) for diagnostics 
and monitoring.  It was endeavored to keep the code size small and code complexity low.  Some design 
decisions toward that end:

* Asynchronous, but single threaded.  Leverages boost::asio's event proactor, and keeps complexity low by 
avoiding many synchronization requirements in handlers and dispatch.  Since the server is asynchronous, 
multiple simultaneous connections are supported robustly, but since it is also single-threaded, throughput 
will likely not scale to hundreds or thousands of simultaneous connections.

* Express and Martini style "middlewares" were not implemented to keep complexity low.  This could be added 
at a later time if desired.  For now, perceived-to-be-commonly-used functionalities are wired directly into 
the server.

#### Features currently supported

* Static content serving out of file system directories, with file-extension-based automatic Content-Type
detection.

* Installable handlers taking Express and Martini style Request and Response objects, and URL path 
specifiers with wildcarding and parameter capture, for conveniently implementing REST services.

* AJAX endpoint helpers: push a JSON string to the server-side endpoint at any time, and all currently 
pending clients will be updated.

* HTTP 1.1 persistent connections.

* Can piggy-back on existing single-threaded asio::io_service instance from hosting application if desired.

#### Overview / Usage

A simple web server, serving static content from a single directory:

```C++
int main(int argc, char *argv[])
{
    boost::asio::io_service service;
    qhttp::Server::Ptr server = qhttp::Server::create(service, 80);
    server->addStaticContent("/*", "/path/to/web/content/dir");
    server->accept();
    service.run();
    return 0;
}
```

Some REST handlers could be added e.g. as follows:

```C++
server.addHandlers({
    {"POST",   "/api/v1/foos",      addFoo    },
    {"GET",    "/api/v1/foos",      listFoos  },
    {"GET",    "/api/v1/foos/:foo", getFoo    },
    {"DELETE", "/api/v1/foos/:foo", deleteFoo }
});
```

addHandlers() is just a convenience wrapper for adding several handlers in one call via an initializer-list.
Handlers are std::function objects, so you can pass function names, lambdas, binds, etc.  An example handler
implementation from above might be:

```C++
void getFoo(qhttp::Request::Ptr req, qhttp::Response::Ptr resp)
{
    string requestedFoo = req->params["foo"];
    auto const& foo = fooMap.find(requestedFoo);
    if (foo == fooMap.end()) {
        resp->sendStatus(404); // simple status response
    } else {
        resp->send(foo->second.toJSON(), "application/json");
    }
}

```

Parameters captured from the URL path are made available in the passed Request::params map.  Parameters
captured from the query portion of the URL are made available in the passed Request::query map.  The passed
Response object provides methods for simple numeric status responses (which will get a default auto-generated
HTML body) or the sending of strings or files.

To install an AJAX endpoint:

```C++
qhttp::AjaxEndpoint::Ptr foosAjax = server->addAjaxEndpoint("/api/v1/foos/ajax");
```

This will then accumulate incoming requests, leaving the associated responses pending.  At any later point,
a JSON string may be pushed to the endpoint:

```C++
foosAjax.update(someJSON);
```

All currently pending requests on the endpoint will be responded to with the JSON, and the endpoint will
reset and begin accumulating incoming requests again.  The update operation is thread safe.

For more details on supported URL patterns, request and response utility methods, etc. please see the
appropriate class headers in this directory.

#### Potential TODO

* POST body parsing for x-www-form-urlencoded.
* Cache control headers.
* Websocket support.
* HTTPS support.
