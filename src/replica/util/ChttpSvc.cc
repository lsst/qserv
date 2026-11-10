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

std::size_t const ChttpSvc::DEFAULT_PAYLOAD_MAX_LENGTH = 4 * 1024 * 1024 * 1024ULL;

ChttpSvc::ChttpSvc(std::string const& context, shared_ptr<ServiceProvider> const& serviceProvider,
                   uint16_t port, size_t maxQueuedRequests, size_t numThreads, size_t payloadMaxLength)
        : _context(context),
          _serviceProvider(serviceProvider),
          _port(port),
          _maxQueuedRequests(maxQueuedRequests),
          _numThreads(numThreads),
          _payloadMaxLength(payloadMaxLength) {
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

    _server->set_payload_max_length(_payloadMaxLength);

    // Thread pool and connection queue configuration.
    // Note that by setting the base and max threads to the same value, we effectively fix the number
    // of threads in the pool and disable so called "dynamic" threading (as per the documentation
    // om the library).
    _server->new_task_queue = [baseThreads = _numThreads, maxThreads = _numThreads,
                               maxQueuedRequests = _maxQueuedRequests] {
        return new httplib::ThreadPool(baseThreads, maxThreads, maxQueuedRequests);
    };

    // Access logging
    _server->set_logger([](httplib::Request const& req, httplib::Response const& res) {
        LOGS(_log, LOG_LVL_INFO, "ChttpSvc: " << req.method << " " << req.path << " -> " << res.status);
    });

    // Error logging
    _server->set_error_logger([](httplib::Error const& err, httplib::Request const* req) {
        string msg = "ChttpSvc: " + httplib::to_string(err) + " while processing request";
        if (req) {
            msg += ", client: " + req->get_header_value("X-Forwarded-For") + ", request: '" + req->method +
                   " " + req->path + " " + req->version + "'" + ", host: " + req->get_header_value("Host");
        }
        LOGS(_log, LOG_LVL_ERROR, msg);
    });

    if (_port == 0) {
        _port = _server->bind_to_any_port(_bindAddr, _port);
        throwIf<runtime_error>(_port < 0, _context + "Failed to bind the server to any port");
    } else {
        bool const bound = _server->bind_to_port(_bindAddr, _port);
        throwIf<runtime_error>(!bound,
                               _context + "Failed to bind the server to the port: " + to_string(_port));
    }
    LOGS(_log, LOG_LVL_INFO,
         _context + "started on port: " + to_string(_port) + " maxQueuedRequests: " +
                 to_string(_maxQueuedRequests) + " numThreads: " + to_string(_numThreads) +
                 " payloadMaxLength: " + to_string(_payloadMaxLength));
}

}  // namespace lsst::qserv::replica
