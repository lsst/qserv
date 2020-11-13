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
#include "replica/IngestHttpSvc.h"

// System headers
#include <functional>
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/IngestHttpSvcMod.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestHttpSvc");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

IngestHttpSvc::Ptr IngestHttpSvc::create(ServiceProvider::Ptr const& serviceProvider,
                                         string const& workerName,
                                         string const& authKey,
                                         string const& adminAuthKey) {
    return IngestHttpSvc::Ptr(new IngestHttpSvc(serviceProvider, workerName, authKey, adminAuthKey));
}


IngestHttpSvc::IngestHttpSvc(ServiceProvider::Ptr const& serviceProvider,
                             string const& workerName,
                             string const& authKey,
                             string const& adminAuthKey)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _authKey(authKey),
        _adminAuthKey(adminAuthKey),
        _io_service() {
}


IngestHttpSvc::~IngestHttpSvc() {
    if (_httpServer != nullptr) _httpServer->stop();
}


void IngestHttpSvc::run() {
    LOGS(_log, LOG_LVL_INFO, _context() << __func__);
    if (_httpServer != nullptr) {
        throw logic_error(_context() + string(__func__) + ": service is already running.");
    }
    auto const self = shared_from_this();
    _httpServer = qhttp::Server::create(
            _io_service,
            _serviceProvider->config()->workerInfo(_workerName).httpLoaderPort);
    _httpServer->addHandlers({
        {"POST", "/ingest/file",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                IngestHttpSvcMod::process(
                        self->_serviceProvider, self->_workerName, self->_authKey, self->_adminAuthKey,
                        req, resp);
            }
        }
    });

    // Make sure the service started before launching any BOOST ASIO threads.
    // This will prevent threads from finishing due to a lack of work to be done.
    _httpServer->start();

    // Launch all threads in the pool
    vector<shared_ptr<thread>> threads(_serviceProvider->config()->httpLoaderNumProcessingThreads());
    for (auto&& ptr: threads) {
        ptr = shared_ptr<thread>(new thread([self]() {
            self->_io_service.run();
        }));
    }
    for (auto&& ptr: threads) {
        ptr->join();
    }
}

}}} // namespace lsst::qserv::replica
