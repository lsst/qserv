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
#include "replica/requests/http/HttpMessenger.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpMessenger");
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpMessenger> HttpMessenger::create(shared_ptr<Config> const& config) {
    return shared_ptr<HttpMessenger>(new HttpMessenger(config));
}

HttpMessenger::HttpMessenger(shared_ptr<Config> const& config) : _config(config) {
    for (auto const& workerName : config->allWorkers()) {
        _workerConnector[workerName] = HttpMessengerConnector::create(config, workerName);
        LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
    }
}

void HttpMessenger::stop() {
    for (auto&& entry : _workerConnector) {
        entry.second->stop();
    }
}
void HttpMessenger::send(string const& workerName, string const& id, int priority, string const& resource,
                         http::Method method, json const& requestJson, OnFinishCallback onFinish,
                         unsigned int timeoutMs) {
    _connector(workerName)->send(id, priority, resource, method, requestJson, onFinish, timeoutMs);
}

void HttpMessenger::cancel(string const& workerName, string const& id) { _connector(workerName)->cancel(id); }

bool HttpMessenger::exists(string const& workerName, string const& id) {
    return _connector(workerName)->exists(id);
}

shared_ptr<HttpMessengerConnector> const& HttpMessenger::_connector(string const& workerName) {
    if (workerName.empty()) {
        throw invalid_argument(_context(workerName) + "worker name is empty");
    }
    replica::Lock lock(_mtx, _context(workerName));
    auto const itr = _workerConnector.find(workerName);
    if (itr != _workerConnector.end()) return itr->second;

    // The worker could be just added to the Config. In this case
    // worker connector needs to be created and registered in the local collection.
    // Note that ConfigUnknownWorker will be thrown by the worker locator method
    // if the name won't match any worker.
    [[maybe_unused]] ConfigWorker const worker = _config->worker(workerName);
    _workerConnector[workerName] = HttpMessengerConnector::create(_config, workerName);
    LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
    return _workerConnector.at(workerName);
}

string HttpMessenger::_context(string const& workerName) {
    return "HTTP MESSENGER [worker=" + workerName + "]  ";
}

}  // namespace lsst::qserv::replica
