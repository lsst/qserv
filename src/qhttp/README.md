# qhttp

This is a somewhat bare-bones C++ HTTP server, inspired by things like Express (http://expressjs.com/) and
Martini (https://github.com/go-martini/martini).

The intended use-case is embedding a light-duty HTTP server within C++ back-end services, to conveniently
provide utility services (ReST APIs, served status pages, small browser applications, etc.) for diagnostics
and monitoring.  It was endeavored to keep the code size small and code complexity low.  Some design decisions
toward that end:

* Leverages boost::asio's event proactor for a fully asyncrhonous server, so multiple simultaneous connections
  are supported robustly even when run in a single thread.  The option to run single-threaded can
  significantly reduce synchronization complications in handler codes, though throughput scaling may be
  limited in this case.

* Express and Martini style "middlewares" were not implemented to keep complexity low.  This could be added at
  a later time if desired.  For now, perceived-to-be-commonly-used functionalities are wired directly into the
  server.

* Parsed HTTP request headers and URL parameters are stored in simple std::maps, rather than std::multimaps,
  which means repeated headers and repeated URL parameters are not supported (only the last parsed instance of
  any given header or URL parameter will be recorded).  This choice was made consciously to keep client code
  simpler, and becuase the repeated header or URL parameter use case was perceived to be uncommon.

#### Features currently supported

* Static content serving out of file system directories, with file-extension-based automatic Content-Type
  detection.

* Installable handlers taking Express and Martini style Request and Response objects, and URL path specifiers
  with wildcarding and parameter capture, for conveniently implementing REST services.

* AJAX endpoint helpers: push a JSON string to the server-side endpoint at any time, and all currently pending
  clients will be updated.

* HTTP 1.1 persistent connections.

* Can piggy-back on existing asio::io_service instance from hosting application if desired.

#### Overview / Usage

A simple web server, serving static content from a single directory:

```C++
int main(int argc, char *argv[])
{
    boost::asio::io_service service;
    boost::system::error_code ec;
    qhttp::Server::Ptr server = qhttp::Server::create(service, 80);
    server->addStaticContent("/*", "/path/to/web/content/dir", ec);
    server->start(ec);
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

The server attempts to be exception-safe, since unhandled exceptions ocurring in asio service threads could be
problematic for a hosting applications.  In particular, any exceptions thrown from user-handler code are
caught by the server and translated to approprate HTTP responses (typically, 500 Internal Server Error).

To install an AJAX endpoint:

```C++
qhttp::AjaxEndpoint::Ptr foosAjax = server->addAjaxEndpoint("/api/v1/foos/ajax");
```

This will then accumulate incoming requests, leaving the associated responses pending.  At any later point, a
JSON string may be pushed to the endpoint:

```C++
foosAjax.update(someJSON);
```

All currently pending requests on the endpoint will be responded to with the JSON, and the endpoint will reset
and begin accumulating incoming requests again.  The update operation is thread safe.

For more details on supported URL patterns, request and response utility methods, etc. please see the
appropriate class headers in this directory.

#### Potential TODO

* POST body parsing for x-www-form-urlencoded.
* Cache control headers.
* Websocket support.
* HTTPS support.
