/*
 * LSST Data Management System
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
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "replica/HttpSvc.h"

// System headers
#include <functional>
#include <stdexcept>
#include <thread>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpSvc");

}  // namespace

namespace lsst::qserv::replica {

HttpSvc::HttpSvc(ServiceProvider::Ptr const& serviceProvider, uint16_t port, unsigned int backlog,
                 size_t numThreads)
        : _serviceProvider(serviceProvider),
          _port(port),
          _backlog(backlog),
          _numThreads(numThreads),
          _io_service_ptr(new boost::asio::io_service()) {}

HttpSvc::~HttpSvc() {
    if (_httpServer != nullptr) _httpServer->stop();
}

void HttpSvc::run() {
    string const context_ = context() + " " + string(__func__) + " ";
    LOGS(_log, LOG_LVL_TRACE, context_);

    if (_httpServer != nullptr) {
        throw logic_error(context_ + "service is already running.");
    }
    _httpServer = qhttp::Server::create(*_io_service_ptr, _port, _backlog);

    // Make sure the services were registered and the server  started before launching
    // any BOOST ASIO threads. This will prevent threads from finishing due to a lack of
    // work to be done.
    registerServices();
    _httpServer->start();

    // Launch all threads in a dedicated pool.
    auto const self = shared_from_this();
    vector<shared_ptr<thread>> threads(_numThreads);
    for (auto&& ptr : threads) {
        ptr = shared_ptr<thread>(new thread([self]() { self->_io_service_ptr->run(); }));
    }
    for (auto&& ptr : threads) {
        ptr->join();
    }
}

void HttpSvc::stop() {
    string const context_ = context() + " " + string(__func__) + " ";
    LOGS(_log, LOG_LVL_TRACE, context_);
    if (_httpServer == nullptr) {
        throw logic_error(context_ + "service is not running.");
    }
    _httpServer->stop();
}

}  // namespace lsst::qserv::replica
