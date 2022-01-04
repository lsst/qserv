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

// Class header
#include "qhttp/Server.h"

// System headers
#include <chrono>
#include <memory>
#include <string>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/regex.hpp"

// Local headers
#include "qhttp/AjaxEndpoint.h"
#include "qhttp/StaticContent.h"

namespace asio = boost::asio;
namespace ip = boost::asio::ip;

using namespace std::literals;

namespace lsst {
namespace qserv {
namespace qhttp {

#define DEFAULT_REQUEST_TIMEOUT 5min


Server::Ptr Server::create(asio::io_service& io_service, unsigned short port, int backlog)
{
    return std::shared_ptr<Server>(new Server(io_service, port, backlog));
}


unsigned short Server::getPort()
{
    return _acceptor.local_endpoint().port();
}


Server::Server(asio::io_service& io_service, unsigned short port, int backlog)
:
    _io_service(io_service),
    _backlog(backlog),
    _acceptorEndpoint(ip::tcp::v4(), port),
    _acceptor(io_service),
    _requestTimeout(DEFAULT_REQUEST_TIMEOUT)
{
}


Server::~Server()
{
    stop();
}


void Server::addHandler(std::string const& method, std::string const& pattern, Handler handler)
{
    auto &handlers = _pathHandlersByMethod[method];
    handlers.resize(handlers.size() + 1);
    auto &phandler = handlers.back();
    phandler.path.parse(pattern);
    phandler.handler = handler;
}


void Server::addHandlers(std::initializer_list<HandlerSpec> handlers)
{
    for(auto& handler: handlers) {
        addHandler(handler.method, handler.pattern, handler.handler);
    }
}


void Server::addStaticContent(std::string const& pattern, std::string const& rootDirectory)
{
    StaticContent::add(*this, pattern, rootDirectory);
}


AjaxEndpoint::Ptr Server::addAjaxEndpoint(const std::string& pattern)
{
    return AjaxEndpoint::add(*this, pattern);
}


void Server::setRequestTimeout(std::chrono::milliseconds const& timeout)
{
    _requestTimeout = timeout;
}


void Server::_accept()
{
    auto socket = std::make_shared<ip::tcp::socket>(_io_service);
    {
        std::lock_guard<std::mutex> lock(_activeSocketsMutex);
        auto removed = std::remove_if(
            _activeSockets.begin(),
            _activeSockets.end(),
            [](auto& weakSocket) {
                return weakSocket.expired();
            }
        );
        _activeSockets.erase(removed, _activeSockets.end());
        _activeSockets.push_back(socket);
    }
    auto self = shared_from_this();
    _acceptor.async_accept(
        *socket,
        [self, socket](boost::system::error_code const& ec) {
            if (!self->_acceptor.is_open() || ec == asio::error::operation_aborted) {
                return;
            }
            if (!ec) {
                boost::system::error_code ignore;
                socket->set_option(ip::tcp::no_delay(true), ignore);
                self->_readRequest(socket);
            }
            self->_accept(); // start accept for the next incoming connection
        }
    );
}


void Server::start()
{
    _acceptor.open(_acceptorEndpoint.protocol());
    _acceptor.set_option(ip::tcp::acceptor::reuse_address(true));
    _acceptor.bind(_acceptorEndpoint);
    _acceptorEndpoint.port(_acceptor.local_endpoint().port()); // preserve assigned port
    _acceptor.listen(_backlog);
    _accept();
}


void Server::stop()
{
    boost::system::error_code ignore;
    _acceptor.close(ignore);
    std::lock_guard<std::mutex> lock(_activeSocketsMutex);
    for(auto& weakSocket : _activeSockets) {
        auto socket = weakSocket.lock();
        if (socket) {
            socket->lowest_layer().shutdown(ip::tcp::socket::shutdown_both, ignore);
            socket->lowest_layer().close(ignore);
        }
    }
    _activeSockets.clear();
}


void Server::_readRequest(std::shared_ptr<ip::tcp::socket> socket)
{
    auto timer = std::make_shared<asio::steady_timer>(_io_service);
    timer->expires_from_now(_requestTimeout);
    timer->async_wait(
        [socket](boost::system::error_code const& ec) {
            if (!ec) {
                boost::system::error_code ignore;
                socket->lowest_layer().shutdown(ip::tcp::socket::shutdown_both, ignore);
                socket->lowest_layer().close(ignore);
            }
        }
    );

    auto self = shared_from_this();
    auto reuseSocket = std::make_shared<bool>(false);
    auto request = std::shared_ptr<Request>(new Request(socket));
    auto response = std::shared_ptr<Response>(new Response(
        socket,
        [self, socket, reuseSocket](boost::system::error_code const& ec, std::size_t sent) {
            if (!ec && *reuseSocket) {
                self->_readRequest(socket);
            }
        }
    ));

    asio::async_read_until(
        *socket, request->_requestbuf, "\r\n\r\n",
        [self, socket, reuseSocket, request, response, timer](
            boost::system::error_code const& ec, size_t bytesRead)
        {
            if (!ec) {
                size_t bytesBuffered = request->_requestbuf.size() - bytesRead;
                request->_parseHeader();
                request->_parseUri();
                if (request->version == "HTTP/1.1") {
                    // Temporary disable this option due to a bug in the implementation
                    // causing disconnect if running the service within the Docker environment.
                    // See: DM-27396
                    //*reuseSocket = true;
                }
                if (request->header.count("Content-Length") > 0) {
                    asio::async_read(
                        *socket, request->_requestbuf,
                        asio::transfer_exactly(
                            stoull(request->header["Content-Length"]) - bytesBuffered
                        ),
                        [self, socket, request, response, timer](
                            boost::system::error_code const& ec, size_t)
                        {
                            timer->cancel();
                            if (!ec) {
                                if (request->header["Content-Type"] == "application/x-www-form-urlencoded") {
                                    request->_parseBody();
                                }
                                self->_dispatchRequest(request, response);
                            }
                        }
                    );
                } else {
                    timer->cancel();
                    self->_dispatchRequest(request, response);
                }
            } else {
                timer->cancel();
            }
        }
    );
}


void Server::_dispatchRequest(Request::Ptr request, Response::Ptr response)
{
    auto pathHandlersIt = _pathHandlersByMethod.find(request->method);
    if (pathHandlersIt != _pathHandlersByMethod.end()) {
        boost::smatch pathMatch;
        for(auto& pathHandler : pathHandlersIt->second) {
            if (boost::regex_match(request->path, pathMatch, pathHandler.path.regex)) {
                pathHandler.path.updateParamsFromMatch(request, pathMatch);
                pathHandler.handler(request, response);
                return;
            }
        }
    }
    response->sendStatus(404);
}

}}} // namespace lsst::qserv::qhttp
