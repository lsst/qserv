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
#include <iostream>
#include <memory>
#include <ratio>
#include <string>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/regex.hpp"

// Local headers
#include "lsst/log/Log.h"
#include "qhttp/AjaxEndpoint.h"
#include "qhttp/LogHelpers.h"
#include "qhttp/StaticContent.h"
#include "qhttp/Status.h"

namespace asio = boost::asio;
namespace errc = boost::system::errc;
namespace ip = boost::asio::ip;
namespace chrono = std::chrono;

using namespace std::literals;

#define DEFAULT_REQUEST_TIMEOUT 5min

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qhttp");
}

namespace lsst::qserv::qhttp {

Server::Ptr Server::create(asio::io_service& io_service, unsigned short port, int backlog) {
    return std::shared_ptr<Server>(new Server(io_service, port, backlog));
}

unsigned short Server::getPort() { return _acceptor.local_endpoint().port(); }

Server::Server(asio::io_service& io_service, unsigned short port, int backlog)
        : _io_service(io_service),
          _backlog(backlog),
          _acceptorEndpoint(ip::tcp::v4(), port),
          _acceptor(io_service),
          _requestTimeout(DEFAULT_REQUEST_TIMEOUT) {}

Server::~Server() { stop(); }

void Server::addHandler(std::string const& method, std::string const& pattern, Handler handler) {
    auto& handlers = _pathHandlersByMethod[method];
    handlers.resize(handlers.size() + 1);
    auto& phandler = handlers.back();
    phandler.path.parse(pattern);
    phandler.handler = handler;
}

void Server::addHandlers(std::initializer_list<HandlerSpec> handlers) {
    for (auto& handler : handlers) {
        addHandler(handler.method, handler.pattern, handler.handler);
    }
}

void Server::addStaticContent(std::string const& pattern, std::string const& rootDirectory) {
    StaticContent::add(*this, pattern, rootDirectory);
}

AjaxEndpoint::Ptr Server::addAjaxEndpoint(const std::string& pattern) {
    return AjaxEndpoint::add(*this, pattern);
}

void Server::setRequestTimeout(chrono::milliseconds const& timeout) { _requestTimeout = timeout; }

void Server::_accept() {
    auto socket = std::make_shared<ip::tcp::socket>(_io_service);
    {
        std::lock_guard<std::mutex> lock(_activeSocketsMutex);
        auto removed = std::remove_if(_activeSockets.begin(), _activeSockets.end(),
                                      [](auto& weakSocket) { return weakSocket.expired(); });
        auto numExpired = _activeSockets.end() - removed;
        if (numExpired != 0) {
            LOGLS_DEBUG(_log, logger(this) << "purging tracking for " << numExpired << " expired socket(s)");
            _activeSockets.erase(removed, _activeSockets.end());
        }
        _activeSockets.push_back(socket);
        LOGLS_DEBUG(_log, logger(this) << "tracking new socket");
    }

    auto self = shared_from_this();
    _acceptor.async_accept(*socket, [self, socket](boost::system::error_code const& ec) {
        if (!self->_acceptor.is_open() || ec == asio::error::operation_aborted) {
            LOGLS_DEBUG(_log, logger(self) << "accept chain exiting");
            return;
        }
        if (!ec) {
            LOGLS_INFO(_log, logger(self) << logger(socket) << "connect from " << socket->remote_endpoint());
            boost::system::error_code ignore;
            socket->set_option(ip::tcp::no_delay(true), ignore);
            self->_readRequest(socket);
        } else {
            LOGLS_ERROR(_log, logger(self) << "accept failed: " << ec.message());
        }
        self->_accept();  // start accept again for the next incoming connection
    });
}

void Server::start() {
    LOGLS_DEBUG(_log, logger(this) << "starting");

    try {
        _acceptor.open(_acceptorEndpoint.protocol());
        _acceptor.set_option(ip::tcp::acceptor::reuse_address(true));
        _acceptor.bind(_acceptorEndpoint);
        _acceptorEndpoint.port(_acceptor.local_endpoint().port());  // preserve assigned port
        _acceptor.listen(_backlog);
    }

    catch (boost::system::system_error const& e) {
        LOGLS_ERROR(_log, logger(this) << "acceptor " << e.what());
        throw e;
    }

    LOGLS_INFO(_log, logger(this) << "listening at " << _acceptor.local_endpoint());
    _accept();
}

void Server::stop() {
    LOGLS_DEBUG(_log, logger(this) << "shutting down");
    boost::system::error_code ignore;
    _acceptor.close(ignore);
    std::lock_guard<std::mutex> lock(_activeSocketsMutex);
    LOGLS_DEBUG(_log, logger(this) << "purging tracking for " << _activeSockets.size() << " socket(s)");
    for (auto& weakSocket : _activeSockets) {
        auto socket = weakSocket.lock();
        if (socket) {
            LOGLS_DEBUG(_log, logger(this) << logger(socket) << "closing");
            socket->lowest_layer().shutdown(ip::tcp::socket::shutdown_both, ignore);
            socket->lowest_layer().close(ignore);
        }
    }
    _activeSockets.clear();
}

void Server::_readRequest(std::shared_ptr<ip::tcp::socket> socket) {
    auto self = shared_from_this();

    // Set up a timer and handler to timeout requests that take too long to arrive.

    auto timer = std::make_shared<asio::steady_timer>(_io_service);
    timer->expires_from_now(_requestTimeout);
    timer->async_wait([self, socket](boost::system::error_code const& ec) {
        if (!ec) {
            LOGLS_WARN(_log, logger(self) << logger(socket) << "read timed out, closing");
            boost::system::error_code ignore;
            socket->lowest_layer().shutdown(ip::tcp::socket::shutdown_both, ignore);
            socket->lowest_layer().close(ignore);
        } else if (ec == asio::error::operation_aborted) {
            LOGLS_DEBUG(_log, logger(self) << logger(socket) << "read timeout timer canceled");
        } else {
            LOGLS_ERROR(_log, logger(self) << logger(socket) << "read timeout timer: " << ec.message());
        }
    });

    // Create Response object for this request. Completion handler will log total request + response
    // time, then either turn-around or close the client socket as appropriate.

    auto startTime = chrono::steady_clock::now();
    auto reuseSocket = std::make_shared<bool>(false);
    auto const response = std::shared_ptr<Response>(new Response(
            self, socket,
            [self, socket, startTime, reuseSocket](boost::system::error_code const& ec, std::size_t sent) {
                chrono::duration<double, std::milli> elapsed = chrono::steady_clock::now() - startTime;
                LOGLS_INFO(_log, logger(self)
                                         << logger(socket) << "request duration " << elapsed.count() << "ms");
                if (!ec && *reuseSocket) {
                    LOGLS_DEBUG(_log, logger(self) << logger(socket) << "lingering");
                    self->_readRequest(socket);
                } else {
                    LOGLS_DEBUG(_log, logger(self) << logger(socket) << "closing");
                    boost::system::error_code ignore;
                    socket->lowest_layer().shutdown(ip::tcp::socket::shutdown_both, ignore);
                    socket->lowest_layer().close(ignore);
                }
            }));

    // Create Request object for this request, and initiate header read.

    auto const request = std::shared_ptr<Request>(new Request(self, socket));
    asio::async_read_until(
            *socket, request->_requestbuf, "\r\n\r\n",
            [self, socket, reuseSocket, request, response, timer](boost::system::error_code const& ec,
                                                                  size_t bytesRead) {
                if (ec == asio::error::operation_aborted) {
                    LOGLS_ERROR(_log, logger(self) << logger(socket) << "header read canceled");
                    timer->cancel();
                    return;
                } else if (ec) {
                    LOGLS_ERROR(_log, logger(self)
                                              << logger(socket) << "header read failed: " << ec.message());
                    timer->cancel();
                    return;
                }

                size_t bytesBuffered = request->_requestbuf.size() - bytesRead;

                if (!(request->_parseHeader() && request->_parseUri())) {
                    timer->cancel();
                    response->sendStatus(STATUS_BAD_REQ);
                    return;
                }

                if (request->version == "HTTP/1.1") {
                    // Temporary disable this option due to a bug in the implementation
                    // causing disconnect if running the service within the Docker environment.
                    // See: DM-27396
                    //*reuseSocket = true;
                }

                if (request->header.count("Content-Length") > 0) {
                    std::size_t bytesToRead;
                    try {
                        bytesToRead = stoull(request->header["Content-Length"]);
                    } catch (std::exception const& e) {
                        LOGLS_WARN(_log, logger(self) << logger(socket)
                                                      << "rejecting request with bad Content-Length: "
                                                      << ctrlquote(request->header["Content-Length"]));
                        timer->cancel();
                        response->sendStatus(STATUS_BAD_REQ);
                        return;
                    }
                    bytesToRead -= bytesBuffered;
                    LOGLS_INFO(_log, logger(self)
                                             << logger(socket) << request->method << " " << request->target
                                             << " " << request->version << " + " << bytesToRead << " bytes");
                    asio::async_read(
                            *socket, request->_requestbuf, asio::transfer_exactly(bytesToRead),
                            [self, socket, request, response, timer](boost::system::error_code const& ec,
                                                                     size_t) {
                                timer->cancel();
                                if (ec == asio::error::operation_aborted) {
                                    LOGLS_ERROR(_log, logger(self) << logger(socket)
                                                                   << "request body read canceled");
                                    return;
                                } else if (ec) {
                                    LOGLS_ERROR(_log,
                                                logger(self) << logger(socket)
                                                             << "request body read failed: " << ec.message());
                                    return;
                                }
                                if ((request->header["Content-Type"] ==
                                     "application/x-www-form-urlencoded") &&
                                    !request->_parseBody()) {
                                    LOGLS_ERROR(_log, logger(self) << logger(socket) << "form decode failed");
                                    response->sendStatus(STATUS_BAD_REQ);
                                    return;
                                }
                                self->_dispatchRequest(request, response);
                            });
                } else {
                    LOGLS_INFO(_log, logger(self) << logger(socket) << request->method << " "
                                                  << request->target << " " << request->version);
                    timer->cancel();
                    self->_dispatchRequest(request, response);
                }
            });
}

void Server::_dispatchRequest(Request::Ptr request, Response::Ptr response) {
    auto pathHandlersIt = _pathHandlersByMethod.find(request->method);
    if (pathHandlersIt != _pathHandlersByMethod.end()) {
        boost::smatch pathMatch;
        for (auto& pathHandler : pathHandlersIt->second) {
            if (boost::regex_match(request->path, pathMatch, pathHandler.path.regex)) {
                pathHandler.path.updateParamsFromMatch(request, pathMatch);
                LOGLS_DEBUG(_log, logger(this) << logger(request->_socket) << "invoking handler for "
                                               << pathHandler.path.regex);
                try {
                    pathHandler.handler(request, response);
                } catch (boost::system::system_error const& e) {
                    LOGLS_ERROR(_log, logger(this) << logger(request->_socket)
                                                   << "exception thrown from handler: " << e.what());
                    switch (e.code().value()) {
                        case errc::permission_denied:
                            response->sendStatus(STATUS_FORBIDDEN);
                            break;
                        default:
                            response->sendStatus(STATUS_INTERNAL_SERVER_ERR);
                            break;
                    }
                } catch (std::exception const& e) {
                    LOGLS_ERROR(_log, logger(this) << logger(request->_socket)
                                                   << "exception thrown from handler: " << e.what());
                    response->sendStatus(STATUS_INTERNAL_SERVER_ERR);
                }
                return;
            }
        }
    }
    LOGLS_DEBUG(_log, logger(this) << logger(request->_socket) << "no handler found");
    response->sendStatus(STATUS_NOT_FOUND);
}

}  // namespace lsst::qserv::qhttp
