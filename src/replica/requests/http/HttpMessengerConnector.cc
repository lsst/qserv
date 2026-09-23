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
#include <exception>
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

#define _THROW_IF_ZERO(param)                                                            \
    if ((param) == 0) {                                                                  \
        throw invalid_argument(_context() + "zero value in " #param " is not allowed."); \
    }

#define _THROW_IF_STOPPED()                                                            \
    if (_stopService) {                                                                \
        throw logic_error(_context() + "service is stopped, cannot process requests"); \
    }

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpMessengerConnector");

using namespace lsst::qserv::replica;

// The thread-local cache of the current HttpMessengerConnector allows to
// prevent worker-thread self-join/destruction hazard. See the destructor of the class.
thread_local HttpMessengerConnector* currentHttpMessengerConnector = nullptr;

class CurrentConnectorGuard {
public:
    explicit CurrentConnectorGuard(HttpMessengerConnector* connector)
            : _previous(currentHttpMessengerConnector) {
        currentHttpMessengerConnector = connector;
    }
    ~CurrentConnectorGuard() { currentHttpMessengerConnector = _previous; }

private:
    HttpMessengerConnector* const _previous;
};

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpMessengerConnector> HttpMessengerConnector::create(
        shared_ptr<Config> const& config, boost::asio::io_service& io_service, string const& workerName,
        shared_ptr<HttpMessengerRequest::IdGenerator> const& requestIdGenerator) {
    auto const ptr = shared_ptr<HttpMessengerConnector>(
            new HttpMessengerConnector(config, io_service, workerName, requestIdGenerator));
    return ptr;
}

bool HttpMessengerConnector::isCurrentThreadWorker() noexcept {
    return currentHttpMessengerConnector != nullptr;
}

HttpMessengerConnector::HttpMessengerConnector(
        shared_ptr<Config> const& config, boost::asio::io_service& io_service, string const& workerName,
        shared_ptr<HttpMessengerRequest::IdGenerator> const& requestIdGenerator)
        : _config(config),
          _io_service(io_service),
          _workerName(workerName),
          _requestIdGenerator(requestIdGenerator),
          _numThreads(config->get<size_t>("controller", "http-messenger-num-threads")) {
    _THROW_IF_EMPTY(_workerName);
    _THROW_IF_ZERO(_numThreads);
}

HttpMessengerConnector::~HttpMessengerConnector() {
    _stop();
    if (currentHttpMessengerConnector != this) return;

    // The last worker reference can be released when a callback destroys the
    // messenger. Detach this worker before its thread object is destroyed.
    lock_guard joinLock(_joinMtx);
    for (auto& thread : _threads) {
        if (!thread->joinable()) continue;
        if (thread->get_id() == this_thread::get_id()) {
            thread->detach();
        } else {
            thread->join();
        }
    }
    _threads.clear();
}

void HttpMessengerConnector::stop() {
    if (currentHttpMessengerConnector == this) {
        _stop();
        auto self = shared_from_this();
        thread([self = move(self)]() { self->_stop(); }).detach();
        return;
    }
    _stop();
}

void HttpMessengerConnector::_stop() {
    {
        lock_guard lock(_mtx);
        _stopService = true;
        for (auto const& [id, request] : _activeRequests) {
            request->cancel();
        }
        _requestQueue.clear();
    }
    _cv.notify_all();
    // A completion callback may stop or destroy the messenger from this worker.
    // Joining here would attempt to join the current thread. Its owning worker
    // reference keeps the connector alive until the loop observes the stop flag.
    if (currentHttpMessengerConnector == this) return;

    lock_guard joinLock(_joinMtx);
    for (auto& thread : _threads) {
        if (thread->joinable()) thread->join();
    }
    _threads.clear();
}

uint64_t HttpMessengerConnector::send(int priority, string const& resource, http::Method method,
                                      json const& requestJson, OnFinishCallback onFinish,
                                      unsigned int timeoutMs) {
    _THROW_IF_EMPTY(resource);
    auto const id = _requestIdGenerator->next();
    {
        lock_guard lock(_mtx);
        _THROW_IF_STOPPED();
        _requestQueue.push_back(make_shared<HttpMessengerRequest>(id, priority, resource, method, requestJson,
                                                                  onFinish, timeoutMs));
    }
    _cv.notify_one();
    return id;
}

void HttpMessengerConnector::cancel(uint64_t id) {
    _THROW_IF_ZERO(id);
    lock_guard lock(_mtx);
    _THROW_IF_STOPPED();
    if (_requestQueue.find(id) != nullptr) {
        _requestQueue.remove(id);
    } else if (_activeRequests.count(id) != 0) {
        auto request = _activeRequests.at(id);
        request->cancel();
    } else {
        throw logic_error(_context() +
                          "no request registered with the specified identifier: " + to_string(id));
    }
}

bool HttpMessengerConnector::exists(uint64_t id) const {
    _THROW_IF_ZERO(id);
    lock_guard lock(_mtx);
    return _requestQueue.find(id) != nullptr || _activeRequests.find(id) != _activeRequests.end();
}

bool HttpMessengerConnector::_processNextRequest() {
    unique_lock lock(_mtx);
    _cv.wait(lock, [this]() { return _stopService || !_requestQueue.empty(); });
    if (_stopService) return false;
    auto request = _requestQueue.front();
    _activeRequests[request->id()] = request;
    lock.unlock();
    bool stopWorker = false;
    try {
        request->process(_baseUrl());
    } catch (ConfigUnknownWorker const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             _context() + "worker was removed from the configuration, stopping request processing");
        stopWorker = true;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, _context() + "request processing failed: " + string(ex.what()));
    } catch (...) {
        LOGS(_log, LOG_LVL_ERROR, _context() + "request processing failed due to an unknown exception");
    }
    lock.lock();
    _activeRequests.erase(request->id());
    lock.unlock();
    return !stopWorker;
}

string HttpMessengerConnector::_baseUrl() const {
    auto const worker = _config->worker(_workerName);
    return "http://" + worker.httpSvcHost.addr + ":" + to_string(worker.httpSvcPort);
}

string HttpMessengerConnector::_context() const { return "HTTP MESSENGER [worker=" + _workerName + "]  "; }

}  // namespace lsst::qserv::replica
