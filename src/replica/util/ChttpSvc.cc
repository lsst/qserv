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
#include "replica/util/ChttpSvc.h"

// System headers
#include <functional>
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/util/Common.h"

// Third-party headers
#include "httplib.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ChttpSvc");

}  // namespace

namespace lsst::qserv::replica {

ChttpSvc::ChttpSvc(std::string const& context, shared_ptr<ServiceProvider> const& serviceProvider,
                   uint16_t port, size_t maxQueuedRequests, size_t numThreads)
        : _context(context),
          _serviceProvider(serviceProvider),
          _port(port),
          _maxQueuedRequests(maxQueuedRequests),
          _numThreads(numThreads) {
    _createAndConfigure();
}

void ChttpSvc::run() {
    // IMPORTANT: Request handlers can't be registered in the constructor
    // since it's not allowed to make calls to shared_from_this() from there.
    registerServices(_server);

    bool const started = _server->listen_after_bind();
    throwIf<runtime_error>(!started, _context + "Failed to start the server");
}

void ChttpSvc::_createAndConfigure() {
    _server = make_unique<httplib::Server>();
    throwIf<runtime_error>(!_server->is_valid(), _context + "Failed to create the server");

    _server->new_task_queue = [&] { return new httplib::ThreadPool(_numThreads, _maxQueuedRequests); };
    if (_port == 0) {
        _port = _server->bind_to_any_port(_bindAddr, _port);
        throwIf<runtime_error>(_port < 0, _context + "Failed to bind the server to any port");
    } else {
        bool const bound = _server->bind_to_port(_bindAddr, _port);
        throwIf<runtime_error>(!bound,
                               _context + "Failed to bind the server to the port: " + to_string(_port));
    }
    LOGS(_log, LOG_LVL_INFO, _context + "started on port " + to_string(_port));
}

}  // namespace lsst::qserv::replica
