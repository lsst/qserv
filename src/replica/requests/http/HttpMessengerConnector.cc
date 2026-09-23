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
#include "replica/requests/http/HttpMessengerConnector.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/config/Config.h"
#include "replica/config/ConfigExceptions.h"
#include "replica/config/ConfigHost.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

#define _THROW_IF_EMPTY(param)                                                   \
    if ((param).empty()) {                                                       \
        throw invalid_argument(_context() + "empty " #param " is not allowed."); \
    }

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpMessengerConnector");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpMessengerConnector> HttpMessengerConnector::create(shared_ptr<Config> const& config,
                                                                  boost::asio::io_service& io_service,
                                                                  string const& workerName) {
    auto const ptr =
            shared_ptr<HttpMessengerConnector>(new HttpMessengerConnector(config, io_service, workerName));
    return ptr;
}

HttpMessengerConnector::HttpMessengerConnector(shared_ptr<Config> const& config,
                                               boost::asio::io_service& io_service, string const& workerName)
        : _config(config), _io_service(io_service), _workerName(workerName) {
    _THROW_IF_EMPTY(_workerName);
}

HttpMessengerConnector::~HttpMessengerConnector() noexcept {
    try {
        cancelAll();
    } catch (...) {
        // Suppress all exceptions to ensure noexcept destructor
    }
}

void HttpMessengerConnector::cancelAll() {
    vector<shared_ptr<HttpMessengerRequest>> requests;
    {
        lock_guard lock(_mtx);
        for (auto const& [id, request] : _activeRequests) {
            requests.push_back(request);
        }
        while (!_requestQueue.empty()) {
            auto request = _requestQueue.front();
            _activeRequests[request->id()] = request;
            requests.push_back(std::move(request));
        }
    }
    for (auto const& request : requests) request->cancel();
}

void HttpMessengerConnector::send(string const& id, int priority, string const& resource, http::Method method,
                                  json const& requestJson, OnFinishCallback onFinish,
                                  unsigned int timeoutSec) {
    _THROW_IF_EMPTY(resource);
    lock_guard lock(_mtx);

    if (_requestQueue.find(id) != nullptr || _activeRequests.find(id) != _activeRequests.end()) {
        throw logic_error(_context() + "request with the specified identifier already exists: " + id);
    }
    _requestQueue.push_back(make_shared<HttpMessengerRequest>(shared_from_this(), _io_service, id, priority,
                                                              resource, method, requestJson, onFinish,
                                                              timeoutSec));
    _processNextRequest(lock);
}

void HttpMessengerConnector::cancel(string const& id) {
    _THROW_IF_EMPTY(id);
    lock_guard lock(_mtx);

    bool notFound = false;
    if (_requestQueue.find(id) != nullptr) {
        auto request = _requestQueue.find(id);
        _requestQueue.remove(id);
        _activeRequests[id] = request;
        request->cancel();
    } else if (_activeRequests.count(id) != 0) {
        auto request = _activeRequests.at(id);
        request->cancel();
    } else {
        notFound = true;
    }

    _processNextRequest(lock);
    if (notFound) {
        throw logic_error(_context() + "no request registered with the specified identifier: " + id);
    }
}

bool HttpMessengerConnector::exists(string const& id) const {
    _THROW_IF_EMPTY(id);
    lock_guard lock(_mtx);
    return _requestQueue.find(id) != nullptr || _activeRequests.find(id) != _activeRequests.end();
}

void HttpMessengerConnector::_processNextRequest(lock_guard<mutex> const& lock) {
    size_t const maxRequests = _config->get<size_t>("controller", "num-requests-per-worker");
    if (_activeRequests.size() >= maxRequests) return;
    if (_requestQueue.empty()) return;

    string baseUrl;
    try {
        baseUrl = _baseUrl();
    } catch (ConfigUnknownWorker const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             _context() + "worker was removed from the configuration, skipping request processing");
        throw;
    }

    // Pull the next available request from the queue, move it to the active requests map,
    // and make an attempt to process it. Note that the request processing method is guaranteed
    // to call its callback and to throw an exception if it fails or succeeds.
    auto request = _requestQueue.front();
    _activeRequests[request->id()] = request;
    try {
        request->process(baseUrl);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, _context() + "request processing failed, ex: " + string(ex.what()));
        throw;
    } catch (...) {
        LOGS(_log, LOG_LVL_ERROR, _context() + "request processing failed due to an unknown exception");
        throw;
    }
}

string HttpMessengerConnector::_baseUrl() const {
    auto const worker = _config->worker(_workerName);
    return "http://" + worker.httpSvcHost.addr + ":" + to_string(worker.httpSvcPort);
}

void HttpMessengerConnector::_processingFinished(string const& id) {
    // Send notification that the request processing has finished outside of the lock
    // to avoid potential deadlocks.
    shared_ptr<HttpMessengerRequest> request;
    {
        lock_guard lock(_mtx);
        auto itr = _activeRequests.find(id);
        if (itr != _activeRequests.end()) {
            request = itr->second;
            _activeRequests.erase(id);
        }
        _processNextRequest(lock);
    }
    if (request != nullptr) request->notify();
}
string HttpMessengerConnector::_context() const { return "HTTP MESSENGER [worker=" + _workerName + "]  "; }

}  // namespace lsst::qserv::replica
