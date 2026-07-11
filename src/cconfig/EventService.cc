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
#include "cconfig/EventService.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "cconfig/DataManagementEvent.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

#define _CONTEXT ("EventService::" + string(__func__) + " ")

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.cconfig.EventService");
}

namespace lsst::qserv::cconfig {

shared_ptr<EventService> EventService::create(size_t numThreads) {
    return shared_ptr<EventService>(new EventService(numThreads));
}

EventService::EventService(size_t numThreads) : _numThreads(numThreads) {
    if (_numThreads == 0) {
        throw invalid_argument(_CONTEXT + "number of threads must be greater than zero.");
    }
}

EventService::~EventService() noexcept {
    if (isRunning()) {
        LOGS(_log, LOG_LVL_WARN, _CONTEXT << "service is still running during destruction. Stopping it.");
        try {
            stop();
        } catch (...) {
        }
    }
}

void EventService::postEvent(DataManagementEvent const& event) {
    if (event.type == DataManagementEvent::Type::NONE) {
        throw invalid_argument(_CONTEXT +
                               "event type cannot be NONE. The event might be default constructed.");
    }
    lock_guard<mutex> lock(_mtx);
    if (_threads.empty()) {
        LOGS(_log, LOG_LVL_WARN, _CONTEXT << "service is not running, event ignored.");
        return;
    }
    for (auto const& [id, callback] : _subscriptions) {
        _io_service->post([callback, event]() { callback(event); });
    }
    LOGS(_log, LOG_LVL_INFO, _CONTEXT << "event posted: " << DataManagementEvent::type2str(event.type));
}

bool EventService::isRunning() const {
    lock_guard<mutex> lock(_mtx);
    return !_threads.empty();
}

void EventService::subscribe(EventService::OnEventCallback const& callback, string const& subscriptionId) {
    if (subscriptionId.empty()) {
        throw invalid_argument(_CONTEXT + "subscription ID cannot be empty.");
    }
    lock_guard<mutex> lock(_mtx);
    if (_subscriptions.find(subscriptionId) != _subscriptions.end()) {
        throw EventServiceException(_CONTEXT + "subscription ID already exists: " + subscriptionId);
    }
    _subscriptions[subscriptionId] = callback;
    LOGS(_log, LOG_LVL_INFO, _CONTEXT << "new subscription added with ID " << subscriptionId);
}

void EventService::unsubscribe(string const& subscriptionId) {
    if (subscriptionId.empty()) {
        throw invalid_argument(_CONTEXT + "subscription ID cannot be empty.");
    }
    lock_guard<mutex> lock(_mtx);
    auto it = _subscriptions.find(subscriptionId);
    if (it == _subscriptions.end()) {
        throw EventServiceException(_CONTEXT + "subscription ID not found: " + subscriptionId);
    }
    _subscriptions.erase(it);
    LOGS(_log, LOG_LVL_INFO, _CONTEXT << "subscription removed with ID " << subscriptionId);
}

void EventService::start() {
    lock_guard<mutex> lock(_mtx);
    if (!_threads.empty()) {
        throw EventServiceException(_CONTEXT + "service is already running.");
    }
    _io_service = make_unique<boost::asio::io_service>();
    _work = make_unique<boost::asio::io_service::work>(*_io_service);

    // Initialize the I/O context and start the service threads. At this point
    // the server will be ready to service incoming requests.
    auto self = shared_from_this();
    for (unsigned int i = 0; i < _numThreads; ++i) {
        _threads.push_back(make_unique<thread>([self]() { self->_io_service->run(); }));
    }
    LOGS(_log, LOG_LVL_INFO, _CONTEXT << "event service started with " << _numThreads << " threads.");
}

void EventService::stop() {
    lock_guard<mutex> lock(_mtx);
    if (_threads.empty()) {
        throw EventServiceException(_CONTEXT + "service is not running.");
    }
    _work.reset();
    _io_service->stop();
    for (auto& thread : _threads) {
        if (thread->joinable()) {
            thread->join();
        }
    }
    _threads.clear();
    LOGS(_log, LOG_LVL_INFO, _CONTEXT << "event service stopped.");
}

}  // namespace lsst::qserv::cconfig
