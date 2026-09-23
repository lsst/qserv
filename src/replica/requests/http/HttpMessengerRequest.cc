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
#include "replica/requests/http/HttpMessengerRequest.h"

// System headers
#include <cstdlib>
#include <unordered_map>

// Qserv headers
#include "http/AsyncReq.h"
#include "replica/requests/http/HttpMessengerConnector.h"

using namespace std;
using namespace nlohmann;

namespace lsst::qserv::replica {

HttpMessengerRequest::HttpMessengerRequest(shared_ptr<HttpMessengerConnector> const& connector,
                                           boost::asio::io_service& io_service, std::string const& id,
                                           int priority, string const& resource, http::Method method,
                                           json const& requestJson, OnFinishCallback onFinish,
                                           unsigned int timeoutMs)
        : _connector(connector),
          _io_service(io_service),
          _id(id),
          _priority(priority),
          _resource(resource),
          _method(method),
          _data(requestJson.is_null() ? "" : requestJson.dump()),
          _onFinish(onFinish),
          _timeoutMs(timeoutMs) {
    if (!onFinish) {
        throw invalid_argument("HTTP messenger request id=" + _id + " must have a valid onFinish callback");
    }
}

bool HttpMessengerRequest::process(string const& baseUrl) {
    lock_guard lock(_mtx);
    if (_asyncReq) return false;

    // The error handling in the request creation logic guarantees that any failure to create
    // the request will be properly reported to the connector.
    auto connector = move(_connector);
    try {
        string const url = baseUrl + _resource;
        _asyncReq = http::AsyncReq::create(
                _io_service,
                [connector, id = _id](decltype(_asyncReq)) { connector->_processingFinished(id); }, _method,
                url, _data, {{"Content-Type", "application/json"}});
    } catch (std::exception const& ex) {
        _error = "failed to create the HTTP request, ex: " + string(ex.what());
        _io_service.post([connector, id = _id] { connector->_processingFinished(id); });
        throw;
    } catch (...) {
        _error = "failed to create the HTTP request, ex: unknown";
        _io_service.post([connector, id = _id] { connector->_processingFinished(id); });
        throw;
    }

    // The start() method throws exceptions after handling them properly (notifying the callback
    // and cleaning up).
    try {
        _asyncReq->setExpirationIval(_timeoutMs * 1000);
        _asyncReq->start();
    } catch (std::logic_error const& ex) {
        // This exception should never happen because it is thrown only if a request is not in
        // AsyncReq::State::CREATED, which is impossible because we just created the AsyncReq object.
        std::abort();
    }
    return true;
}

void HttpMessengerRequest::cancel() {
    lock_guard lock(_mtx);
    // If the request has not been started yet, notify the connector that it has been canceled.
    if (!_asyncReq) {
        // If the connector is no longer available, it means the connector
        // was notified already, so there is nothing to do here.
        if (!_connector) return;
        _io_service.post([id = _id, connector = move(_connector)] { connector->_processingFinished(id); });
    } else {
        // If the request object is valid then it will send the notification to the connector
        // upon completion (either success or cancellation).
        _asyncReq->cancel();
    }
}

void HttpMessengerRequest::notify() {
    lock_guard lock(_mtx);
    if (!_onFinish) return;
    _io_service.post([asyncReq = move(_asyncReq), onFinish = move(_onFinish), msg = _error] {
        onFinish(asyncReq, msg);
    });
}
}  // namespace lsst::qserv::replica
