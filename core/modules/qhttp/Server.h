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
    using Handler = std::function<void(Request::Ptr, Response::Ptr)>;

    using HandlerSpec = struct {
        std::string const& method;
        std::string const& pattern;
        Handler handler;
    };

    static Ptr create(boost::asio::io_service& io_service, unsigned short port);
    unsigned short getPort(); // useful to retrieve OS assigned port if port "0" passed to create()

    void addHandler(std::string const& method, std::string const& pattern, Handler handler);
    void addHandlers(std::initializer_list<HandlerSpec> handlers);

    void addStaticContent(std::string const& path, std::string const& rootDirectory);
    AjaxEndpoint::Ptr addAjaxEndpoint(std::string const& path);

    void accept(); // install head of asio handler chain for this server

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

};

}}} // namespace lsst::qserv::qhttp

#endif    // LSST_QSERV_QHTTP_SERVER_H
