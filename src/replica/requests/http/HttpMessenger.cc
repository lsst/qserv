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
#include <exception>
#include <thread>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/config/ConfigExceptions.h"
#include "replica/config/ConfigWorker.h"
#include "replica/requests/http/HttpMessengerConnector.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpMessenger");
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpMessenger> HttpMessenger::create(shared_ptr<Config> const& config,
                                                boost::asio::io_service& io_service) {
    return shared_ptr<HttpMessenger>(new HttpMessenger(config, io_service));
}

HttpMessenger::HttpMessenger(shared_ptr<Config> const& config, boost::asio::io_service& io_service)
        : _config(config), _io_service(io_service) {
    for (auto const& workerName : config->allWorkers()) {
        [[maybe_unused]] auto const connector = _add(workerName);
    }
}

HttpMessenger::~HttpMessenger() noexcept {
    try {
        cancelAll();
    } catch (...) {
        // Suppress all exceptions to ensure noexcept destructor
    }
}

void HttpMessenger::cancelAll() {
    lock_guard lock(_mtx);
    for (auto const& [workerName, connector] : _workerConnector) {
        connector->cancelAll();
    }
}
void HttpMessenger::send(string const& workerName, string const& id, int priority, string const& resource,
                         http::Method method, json const& requestJson, OnFinishCallback onFinish,
                         unsigned int timeoutSec) {
    _connector(workerName)->send(id, priority, resource, method, requestJson, onFinish, timeoutSec);
}

void HttpMessenger::cancel(string const& workerName, string const& id) { _connector(workerName)->cancel(id); }

bool HttpMessenger::exists(string const& workerName, string const& id) {
    return _connector(workerName)->exists(id);
}

json HttpMessenger::statsToJson() const {
    lock_guard lock(_mtx);
    json result = json::object();
    for (auto const& [workerName, connector] : _workerConnector) {
        result[workerName] = connector->statsToJson();
    }
    return result;
}

shared_ptr<HttpMessengerConnector> HttpMessenger::_connector(string const& workerName) {
    if (workerName.empty()) {
        throw invalid_argument(_context(workerName) + "worker name is empty");
    }
    shared_ptr<HttpMessengerConnector> removedConnector;
    exception_ptr workerLookupError;
    {
        lock_guard lock(_mtx);
        auto const itr = _workerConnector.find(workerName);
        if (itr != _workerConnector.end()) {
            // Make sure the connector is valid before returning it.
            try {
                [[maybe_unused]] ConfigWorker const worker = _config->worker(workerName);
                return itr->second;
            } catch (ConfigUnknownWorker const&) {
                LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector removed due to unknown worker");
                removedConnector = itr->second;
                _workerConnector.erase(itr);
                workerLookupError = current_exception();
            }
        } else {
            // The worker could have just been added to the Config.
            auto const connector = _add(workerName);
            if (connector == nullptr) {
                throw HttpMessengerInvalidConnection(_context(workerName) + "failed to add connector");
            }
            return connector;
        }
    }
    removedConnector->cancelAll();
    rethrow_exception(workerLookupError);
}

shared_ptr<HttpMessengerConnector> HttpMessenger::_add(string const& workerName) {
    auto const worker = _config->worker(workerName);
    if (worker.httpSvcPort != 0 && !worker.httpSvcHost.addr.empty()) {
        auto connector = HttpMessengerConnector::create(_config, _io_service, workerName);
        _workerConnector[workerName] = connector;
        LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
        return connector;
    }
    LOGS(_log, LOG_LVL_WARN,
         _context(workerName) << "connector not added due to missing HTTP service parameters");
    return nullptr;
}

string HttpMessenger::_context(string const& workerName) {
    return "HTTP MESSENGER [worker=" + workerName + "]  ";
}

}  // namespace lsst::qserv::replica
