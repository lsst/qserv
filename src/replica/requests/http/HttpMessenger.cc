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
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/config/ConfigExceptions.h"
#include "replica/requests/http/HttpMessengerConnector.h"

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

HttpMessenger::HttpMessenger(shared_ptr<Config> const& config)
        : _config(config), _requestIdGenerator(make_shared<HttpMessengerRequest::IdGenerator>()) {
    for (auto const& workerName : config->allWorkers()) {
        _workerConnector[workerName] =
                HttpMessengerConnector::create(config, workerName, _requestIdGenerator);
        LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
    }
}

HttpMessenger::~HttpMessenger() noexcept {
    try {
        stop();
    } catch (...) {
    }
}

void HttpMessenger::stop() {
    // Make a local copy of the connectors to stop them outside of the lock.
    // This prevents possible deadlocks should the stop operation involve any callbacks
    // that might try to acquire the same lock.
    vector<shared_ptr<HttpMessengerConnector>> connectors;
    {
        replica::Lock lock(_mtx, "HTTP MESSENGER stop");
        if (_stopService.exchange(true)) return;
        for (auto&& entry : _workerConnector) {
            connectors.push_back(entry.second);
        }
    }
    if (HttpMessengerConnector::isCurrentThreadWorker()) {
        thread([connectors = move(connectors)]() mutable {
            for (auto&& connector : connectors) {
                connector->stop();
            }
        }).detach();
        return;
    }
    for (auto&& connector : connectors) {
        connector->stop();
    }
}
uint64_t HttpMessenger::send(string const& workerName, int priority, string const& resource,
                             http::Method method, json const& requestJson, OnFinishCallback onFinish,
                             unsigned int timeoutMs) {
    return _connector(workerName)->send(priority, resource, method, requestJson, onFinish, timeoutMs);
}

void HttpMessenger::cancel(string const& workerName, uint64_t id) { _connector(workerName)->cancel(id); }

bool HttpMessenger::exists(string const& workerName, uint64_t id) {
    return _connector(workerName)->exists(id);
}

shared_ptr<HttpMessengerConnector> HttpMessenger::_connector(string const& workerName) {
    if (workerName.empty()) {
        throw invalid_argument(_context(workerName) + "worker name is empty");
    }
    shared_ptr<HttpMessengerConnector> removedConnector;
    exception_ptr workerLookupError;
    {
        replica::Lock lock(_mtx, _context(workerName));
        if (_stopService) {
            throw logic_error(_context(workerName) + "service is stopped, cannot send new requests");
        }
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
            [[maybe_unused]] ConfigWorker const worker = _config->worker(workerName);
            auto connector = HttpMessengerConnector::create(_config, workerName, _requestIdGenerator);
            _workerConnector[workerName] = connector;
            LOGS(_log, LOG_LVL_INFO, _context(workerName) << "connector added");
            return connector;
        }
    }
    removedConnector->stop();
    rethrow_exception(workerLookupError);
}

string HttpMessenger::_context(string const& workerName) {
    return "HTTP MESSENGER [worker=" + workerName + "]  ";
}

}  // namespace lsst::qserv::replica
