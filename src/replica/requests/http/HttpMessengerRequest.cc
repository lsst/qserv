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
#include <unordered_map>

// Qserv headers
#include "http/AsyncReq.h"

using namespace std;

namespace lsst::qserv::replica {

HttpMessengerRequest::HttpMessengerRequest(boost::asio::io_service& io_service, uint64_t id, int priority, string const& resource,
                                           http::Method method, json const& requestJson,
                                           OnFinishCallback onFinish, unsigned int timeoutMs)
        : _io_service(io_service),
          _id(id),
          _priority(priority),
          _resource(resource),
          _method(method),
          _data(requestJson.is_null() ? "" : requestJson.dump()),
          _onFinish(onFinish),
          _timeoutMs(timeoutMs) {
    if (!onFinish) {
        throw invalid_argument("HTTP messenger request id=" + to_string(_id) + " must have a valid onFinish callback");
    }
}

void HttpMessengerRequest::process(string const& baseUrl) {
    string const url = baseUrl + _resource;
    lock_guard lock(_mtx);
    if (_asyncReq != nullptr) {
        throw logic_error("HTTP messenger request id=" + to_string(_id) + " is already in progress");
    }
    _asyncReq = http::AsyncReq::create(_io_service, [self = shared_from_this(), onFinish = move(_onFinish)]() {
            onFinish(self->_id, self->_asyncReq);
        },
        _method, url, _data, {{"Content-Type", "application/json"}});
    _asyncReq->setExpirationIval(_timeoutMs * 1000);
    _asyncReq->start();
}

void HttpMessengerRequest::cancel() {
    lock_guard lock(_mtx);
    if (_asyncReq == nullptr) {
        if (_onFinish) {
            _io_service.post([self = shared_from_this(), onFinish = move(_onFinish)] {
                onFinish(self->_id, nullptr);
            });
        }
    } else {
        _asyncReq->cancel();
    }
}

}  // namespace lsst::qserv::replica
