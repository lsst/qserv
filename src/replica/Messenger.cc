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
#include "replica/Messenger.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Messenger");
}  // namespace

namespace lsst { namespace qserv { namespace replica {

Messenger::Ptr Messenger::create(ServiceProvider::Ptr const& serviceProvider,
                                 boost::asio::io_service& io_service) {
    return Messenger::Ptr(new Messenger(serviceProvider, io_service));
}

Messenger::Messenger(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service)
        : _serviceProvider(serviceProvider), _io_service(io_service) {
    for (auto&& worker : serviceProvider->config()->allWorkers()) {
        _workerConnector[worker] = MessengerConnector::create(serviceProvider, io_service, worker);
        LOGS(_log, LOG_LVL_INFO, _context(worker) << "connector added");
    }
}

void Messenger::stop() {
    for (auto&& entry : _workerConnector) {
        entry.second->stop();
    }
}

void Messenger::cancel(string const& worker, string const& id) { _connector(worker)->cancel(id); }

bool Messenger::exists(string const& worker, string const& id) { return _connector(worker)->exists(id); }

MessengerConnector::Ptr const& Messenger::_connector(string const& worker) {
    util::Lock lock(_mtx, _context(worker));

    if (auto const itr = _workerConnector.find(worker); itr != _workerConnector.end()) {
        return itr->second;
    }

    // The worker could be just added to the Configuration. In this case
    // worker connector needs to be created and registered in the local collection.
    // Note that std::invalid_argument will be thrown by the worker locator method
    // if the name won't match any worker.
    WorkerInfo const workerInfo = _serviceProvider->config()->workerInfo(worker);
    auto const [itr, success] = _workerConnector.insert(
            {worker, MessengerConnector::create(_serviceProvider, _io_service, worker)});
    if (success) {
        LOGS(_log, LOG_LVL_INFO, _context(worker) << "connector added");
        return itr->second;
    }
    throw runtime_error(_context(worker) + "failed to register the connector.");
}

string Messenger::_context(string const& worker) { return "MESSENGER [worker=" + worker + "]  "; }

}}}  // namespace lsst::qserv::replica
