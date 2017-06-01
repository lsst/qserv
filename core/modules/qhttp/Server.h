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

namespace lsst {
namespace qserv {
namespace qhttp {

class Server : public std::enable_shared_from_this<Server>
{
public:

    using Ptr = std::shared_ptr<Server>;

    //----- The server dispatches incoming HTTP requests to Handlers.  A Handler is a callable that receives
    //      shared ptrs to Request and Response objects.

    using Handler = std::function<void(Request::Ptr, Response::Ptr)>;

    //----- Handlers are installed on a server for a given HTTP method ("GET", "PUT", "POST", etc.), and with
    //      a pattern of URI's to match against.  The pattern handling code is a fairly straight port of
    //      path-to-regexp (https://github.com/pillarjs/path-to-regexp) as used by express.js; see that link
    //      for examples of supported pattern syntax. A HandlerSpec combines a Handler, an HTTP method, and
    //      a URI pattern for convenient installation of multiple handlers via std::initializer_lists.

    using HandlerSpec = struct {
        std::string const& method;
        std::string const& pattern;
        Handler handler;
    };

    //----- create() is a static Server factory method.  Pass in an asio::io_service instance onto which the
    //      constructed server will install asynchronous event handlers as necessary.  Optionally pass a TCP
    //      port on which the server should listen for incoming requests; if 0 is passed as the port, a free
    //      port will be selected by the operating system (in which case getPort() may subsequently be called
    //      to discover the assigned port).

    static Ptr create(boost::asio::io_service& io_service, unsigned short port);
    unsigned short getPort();

    //----- Methods to install Handlers on a Server

    void addHandler(std::string const& method, std::string const& pattern, Handler handler);
    void addHandlers(std::initializer_list<HandlerSpec> handlers);

    //----- StaticContent and AjaxEndpoint are specialized Handlers for common use cases (static files served
    //      from a single root directory and thread-safe multi-client AJAX respectively).  See associated
    //      header files for details.  Convenience functions are provided here to instantiate and install
    //      these.

    void addStaticContent(std::string const& path, std::string const& rootDirectory);
    AjaxEndpoint::Ptr addAjaxEndpoint(std::string const& path);

    //----- setRequestTimeout() allows the user to override the default 5 minute start-of-request to
    //      end-of-response timeout.  Must be called before accept().

    void setRequestTimeout(std::chrono::milliseconds const& timeout);

    //----- accept() installs the head of the asynchronous even handler chain onto the asio::io_service
    //      provided when the Server instance was constructed.  Event handlers for the Server tail out when
    //      asio::io_service::stop() is subsquently called to shutdown that io_service.

    void accept();

private:

    Server(Server const&) = delete;
    Server& operator=(Server const&) = delete;

    Server(boost::asio::io_service& io_service, unsigned short port);

    void _readRequest(std::shared_ptr<boost::asio::ip::tcp::socket> socket);
    void _dispatchRequest(Request::Ptr request, Response::Ptr response);

    struct PathHandler {
        Path path;
        Handler handler;
    };

    std::unordered_map<std::string, std::vector<PathHandler>> _pathHandlersByMethod;

    boost::asio::io_service& _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;

    std::chrono::milliseconds _requestTimeout;

};

}}} // namespace lsst::qserv::qhttp

#endif    // LSST_QSERV_QHTTP_SERVER_H
