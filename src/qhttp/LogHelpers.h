/*
 * LSST Data Management System
 * Copyright 2022 AURA/LSST.
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

#ifndef LSST_QSERV_QHTTP_LOGHELPERS_H
#define LSST_QSERV_QHTTP_LOGHELPERS_H

// System headers
#include <iostream>
#include <memory>

// Local headers
#include "qhttp/AjaxEndpoint.h"
#include "qhttp/Server.h"

// Third-party headers
#include "boost/asio.hpp"

namespace lsst {
namespace qserv {
namespace qhttp {

//
//----- Helper ouput stream manipulators for logging.  These let you say things like:
//      logger(self) << logger(sock) << "log message..." in logger macro calls
//

struct ServerLogger
{
    Server const* server;
    ServerLogger(Server const* server) : server(server) {};
};

inline std::ostream& operator<<(std::ostream& str, ServerLogger const& logger) {
    return str << "srv=" << logger.server << " ";
}

inline ServerLogger logger(Server const* server) { return ServerLogger(server); }
inline ServerLogger logger(Server::Ptr const& server) { return ServerLogger(server.get()); }

struct SocketLogger
{
    boost::asio::detail::socket_type sockno;
    SocketLogger(boost::asio::ip::tcp::socket* socket) : sockno(socket->native_handle()) {};
};

inline std::ostream& operator<<(std::ostream& str, SocketLogger const& logger) {
    return str << "sock=" << logger.sockno << " ";
}

inline SocketLogger logger(std::shared_ptr<boost::asio::ip::tcp::socket> socket)
{
    return SocketLogger(socket.get());
}

struct AjaxLogger
{
    AjaxEndpoint const* aep;
    AjaxLogger(AjaxEndpoint const* aep) : aep(aep) {};
};

inline std::ostream& operator<<(std::ostream& str, AjaxLogger const& logger) {
    return str << "ajax=" << logger.aep << " ";
}

inline AjaxLogger logger(AjaxEndpoint const* aep) { return AjaxLogger(aep); }
inline AjaxLogger logger(AjaxEndpoint::Ptr const& aep) { return AjaxLogger(aep.get()); }

}}} // namespace lsst::qserv::qhttp

#endif // !defined(LSST_QSERV_QHTTP_LOGHELPERS_H)
