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
#include "replica/requests/Messenger.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Messenger");
}  // namespace

namespace lsst::qserv::replica {

Messenger::Ptr Messenger::create(shared_ptr<Configuration> const& config,
                                 boost::asio::io_service& io_service) {
    return Messenger::Ptr(new Messenger(config, io_service));
}

Messenger::Messenger(shared_ptr<Configuration> const& config, boost::asio::io_service& io_service)
        : _config(config), _io_service(io_service) {
    for (auto&& workerName : config->allWorkers()) {
        _workerConnector[workerName] = MessengerConnector::create(config, io_service, workerName);
        LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
    }
}

void Messenger::stop() {
    for (auto&& entry : _workerConnector) {
        entry.second->stop();
    }
}

void Messenger::cancel(string const& workerName, string const& id) { _connector(workerName)->cancel(id); }

bool Messenger::exists(string const& workerName, string const& id) {
    return _connector(workerName)->exists(id);
}

MessengerConnector::Ptr const& Messenger::_connector(string const& workerName) {
    replica::Lock lock(_mtx, _context(workerName));

    if (auto const itr = _workerConnector.find(workerName); itr != _workerConnector.end()) {
        return itr->second;
    }

    // The worker could be just added to the Configuration. In this case
    // worker connector needs to be created and registered in the local collection.
    // Note that std::invalid_argument will be thrown by the worker locator method
    // if the name won't match any worker.
    [[maybe_unused]] ConfigWorker const worker = _config->worker(workerName);
    auto const [itr, success] = _workerConnector.insert(
            {workerName, MessengerConnector::create(_config, _io_service, workerName)});
    if (success) {
        LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
        return itr->second;
    }
    throw runtime_error(_context(workerName) + "failed to register the connector.");
}

string Messenger::_context(string const& workerName) { return "MESSENGER [worker=" + workerName + "]  "; }

}  // namespace lsst::qserv::replica
