/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_QSERV_QHTTP_SERVER_H
#define LSST_QSERV_QHTTP_SERVER_H

// System headers
#include <chrono>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"

// Local headers
#include "qhttp/AjaxEndpoint.h"
#include "qhttp/Path.h"
#include "qhttp/Response.h"
#include "qhttp/Request.h"

namespace lsst::qserv::qhttp {

class Server : public std::enable_shared_from_this<Server> {
public:
    using Ptr = std::shared_ptr<Server>;

    //----- The server dispatches incoming HTTP requests to Handlers.  A Handler is a callable that receives
    //      shared ptrs to Request and Response objects.  Exceptions thrown from handlers will be caught by
    //      the server and translated to appropriate HTTP error responses (typically 500 Internal Server
    //      Error).

    using Handler = std::function<void(Request::Ptr, Response::Ptr)>;

    //----- Handlers are installed on a server for a given HTTP method ("GET", "PUT", "POST", etc.), and with
    //      a pattern of URI's to match against.  The pattern handling code is a fairly straight port of
    //      path-to-regexp (https://github.com/pillarjs/path-to-regexp) as used by express.js; see that link
    //      for examples of supported pattern syntax. A HandlerSpec combines a Handler, an HTTP method, and
    //      a URI pattern for convenient installation of multiple handlers via std::initializer_lists.
    //      The optional readEntireBody parameter of HandlerSpec allows the user to specify whether the
    //      server should read the entire request body before calling the handler.  If false, the handler
    //      will be called with a request object that has a null body, and the handler is responsible for
    //      reading the body as necessary.  This is useful for handlers that need to stream large request
    //      bodies to disk or to another service.

    using HandlerSpec = struct {
        std::string const& method;
        std::string const& pattern;
        Handler handler;
        bool readEntireBody = true;
    };

    //----- create() is a static Server factory method.  Pass in an asio::io_service instance onto which the
    //      constructed server will install asynchronous event handlers as necessary.  Optionally pass a TCP
    //      port on which the server should listen for incoming requests; if 0 is passed as the port, a free
    //      port will be selected by the operating system (in which case getPort() may subsequently be called
    //      after start() to discover the assigned port). Parameter 'backlog' of the method specifies the
    //      maximum length of the queue of pending connections.

    static Ptr create(boost::asio::io_service& io_service, unsigned short port,
                      int backlog = boost::asio::socket_base::max_listen_connections,
                      std::size_t const _maxResponseBufSize = 0);
    unsigned short getPort();

    ~Server();

    //----- Methods to install Handlers on a Server.  These must be called before start(), or between calls
    //      to stop() and start().

    void addHandler(std::string const& method, std::string const& pattern, Handler handler,
                    bool readEntireBody = true);
    void addHandlers(std::initializer_list<HandlerSpec> handlers);

    //----- StaticContent and AjaxEndpoint are specialized Handlers for common use cases (static files served
    //      from a single root directory and thread-safe multi-client AJAX respectively).  See associated
    //      header files for details.  Convenience functions are provided here to instantiate and install
    //      these.

    void addStaticContent(std::string const& pattern, std::string const& rootDirectory);
    AjaxEndpoint::Ptr addAjaxEndpoint(std::string const& pattern);

    //----- setRequestTimeout() allows the user to override the default 5 minute start-of-request to
    //      end-of-response timeout.  Must be called before start(), or between calls to stop() and start().

    void setRequestTimeout(std::chrono::milliseconds const& timeout);

    //----- setMaxResponseBufSize() allows the user to override the default (or explicitly specified in
    //      the class's constructor) size of the response buffer. There are no restricton on when the method
    //      is called. The change will affect next requests processed after making the call.
    //      Passing 0 as a value of the parameter will reset the buffer size to the implementation default.

    void setMaxResponseBufSize(std::size_t const maxResponseBufSize);

    //----- start() opens the server listening socket and installs the head of the asynchronous event
    //      handler chain onto the asio::io_service provided when the Server instance was constructed.
    //      Server execution may be halted either calling stop(), or by calling asio::io_service::stop()
    //      on the associated asio::io_service.

    void start();

    //----- stop() shuts down the server by closing all active sockets, including the server listening
    //      socket.  No new connections will be accepted, and handlers in progress will err out the next
    //      time they try to read/write from/to their client sockets.  A call to start() will be needed to
    //      resume server operation.

    void stop();

private:
    friend class Request;

    struct PathHandler {
        Path path;
        Handler handler;
        bool readEntireBody = true;
    };

    Server(Server const&) = delete;
    Server& operator=(Server const&) = delete;

    Server(boost::asio::io_service& io_service, unsigned short port, int backlog,
           std::size_t const _maxResponseBufSize);

    void _accept();

    std::shared_ptr<boost::asio::steady_timer> _startTimer(
            std::shared_ptr<boost::asio::ip::tcp::socket> socket);
    void _readRequest(std::shared_ptr<boost::asio::ip::tcp::socket> socket);
    std::shared_ptr<Server::PathHandler> _findPathHandler(Request::Ptr request);
    void _dispatchRequest(std::shared_ptr<PathHandler> pathHandler, Request::Ptr request,
                          Response::Ptr response);

    std::unordered_map<std::string, std::vector<PathHandler>> _pathHandlersByMethod;

    boost::asio::io_service& _io_service;

    int const _backlog;
    std::size_t _maxResponseBufSize;
    boost::asio::ip::tcp::endpoint _acceptorEndpoint;
    boost::asio::ip::tcp::acceptor _acceptor;

    std::chrono::milliseconds _requestTimeout;

    std::vector<std::weak_ptr<boost::asio::ip::tcp::socket>> _activeSockets;
    std::mutex _activeSocketsMutex;
};

}  // namespace lsst::qserv::qhttp

#endif  // LSST_QSERV_QHTTP_SERVER_H
