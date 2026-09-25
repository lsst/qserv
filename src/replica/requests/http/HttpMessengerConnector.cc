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
#include "http/Client.h"
#include "http/ClientConfig.h"
#include "http/ClientConnPool.h"
#include "replica/config/Config.h"
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

HttpMessengerRequest::HttpMessengerRequest(string const& id, int priority, string const& resource,
                                           http::Method method, json const& requestJson,
                                           OnFinishCallback onFinish, unsigned int timeoutMs)
        : _id(id),
          _priority(priority),
          _resource(resource),
          _method(method),
          _requestJson(requestJson),
          _onFinish(onFinish),
          _timeoutMs(timeoutMs) {}

void HttpMessengerRequest::process(string const& baseUrl, shared_ptr<http::ClientConnPool> const& connPool) {
    http::ClientConfig clientConfig;
    if (_timeoutMs > 0) clientConfig.timeoutMs = _timeoutMs;
    string const url = baseUrl + _resource;
    vector<string> const headers = {"Content-Type: application/json"};
    http::Client client(_method, url, _requestJson.dump(), headers, clientConfig, connPool);
    json responseJson;
    bool success = false;
    string errorMsg;
    try {
        responseJson = client.readAsJson();
        success = true;
    } catch (std::exception const& ex) {
        errorMsg = ex.what();
    }
    if (!_canceled) {
        _onFinish(_id, success, errorMsg, responseJson);
    }
}

HttpMessengerConnector::HttpMessengerConnector(shared_ptr<Config> const& config, string const& workerName)
        : _worker(config->worker(workerName)),
          _baseUrl("http://" + _worker.httpSvcHost.addr + ":" + to_string(_worker.httpSvcPort)),
          _numThreads(config->get<size_t>("controller", "http-messenger-num-threads")),
          _connPool(new http::ClientConnPool(_numThreads)) {}

shared_ptr<HttpMessengerConnector> HttpMessengerConnector::create(shared_ptr<Config> const& config,
                                                                  string const& workerName) {
    auto const ptr = shared_ptr<HttpMessengerConnector>(new HttpMessengerConnector(config, workerName));
    ptr->_init();
    return ptr;
}

void HttpMessengerConnector::stop() {
    unique_lock lock(_mtx);
    _stopService = true;
    _requestQueue.clear();
    for (auto const& [id, request] : _activeRequests) {
        request->cancel();
    }
    lock.unlock();
    _cv.notify_all();
}

void HttpMessengerConnector::send(string const& id, int priority, string const& resource, http::Method method,
                                  json const& requestJson, OnFinishCallback onFinish,
                                  unsigned int timeoutMs) {
    _THROW_IF_EMPTY(id)
    _THROW_IF_EMPTY(resource)
    unique_lock lock(_mtx);
    auto request = make_shared<HttpMessengerRequest>(id, priority, resource, method, requestJson, onFinish,
                                                     timeoutMs);
    _requestQueue.push_back(request);
    lock.unlock();
    _cv.notify_one();
}

void HttpMessengerConnector::cancel(string const& id) {
    _THROW_IF_EMPTY(id)
    lock_guard lock(_mtx);
    if (_requestQueue.find(id) != nullptr) {
        _requestQueue.remove(id);
    } else if (_activeRequests.count(id) != 0) {
        auto request = _activeRequests.at(id);
        request->cancel();
        _activeRequests.erase(id);
    }
}

bool HttpMessengerConnector::exists(string const& id) const {
    _THROW_IF_EMPTY(id)
    lock_guard lock(_mtx);
    return _requestQueue.find(id) != nullptr || _activeRequests.find(id) != _activeRequests.end();
}

void HttpMessengerConnector::_init() {
    lock_guard lock(_mtx);
    auto self = shared_from_this();
    for (size_t i = 0; i < _numThreads; ++i) {
        _threads.push_back(make_unique<thread>([self]() { self->_processRequests(); }));
    }
}

void HttpMessengerConnector::_processRequests() {
    while (!_stopService) {
        unique_lock lock(_mtx);
        _cv.wait(lock, [this]() { return _stopService || !_requestQueue.empty(); });
        if (_stopService) break;
        auto request = _requestQueue.front();
        _activeRequests[request->id()] = request;
        lock.unlock();
        request->process(_baseUrl, _connPool);
        lock.lock();
        _activeRequests.erase(request->id());
        lock.unlock();
    }
}

string HttpMessengerConnector::_context() const { return "HTTP MESSENGER [worker=" + _worker.name + "]  "; }

}  // namespace lsst::qserv::replica
