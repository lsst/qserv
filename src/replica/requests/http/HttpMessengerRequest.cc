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
#include <stdexcept>
#include <thread>
#include <vector>
#include <unordered_map>

// Qserv headers
#include "http/AsyncReq.h"

// LSST headers
#include "lsst/log/Log.h"

// Third-party headers
#include "boost/asio.hpp"

using namespace nlohmann;
using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpMessengerRequest");
}  // namespace

namespace lsst::qserv::replica {

HttpMessengerRequest::HttpMessengerRequest(uint64_t id, int priority, string const& resource,
                                           http::Method method, json const& requestJson,
                                           OnFinishCallback onFinish, unsigned int timeoutMs)
        : _id(id),
          _priority(priority),
          _resource(resource),
          _method(method),
          _requestJson(requestJson),
          _onFinish(onFinish),
          _timeoutMs(timeoutMs) {}

void HttpMessengerRequest::process(string const& baseUrl) {
    string const url = baseUrl + _resource;
    unordered_map<std::string, std::string> const headerMap = {{"Content-Type", "application/json"}};
    json responseJson;
    bool success = false;
    string errorMsg;
    try {
        boost::asio::io_service ioService;
        vector<thread> serviceThreads;
        for (size_t i = 0; i < 2; ++i) {
            serviceThreads.emplace_back([&ioService]() {
                boost::asio::io_service::work work(ioService);
                ioService.run();
            });
        }
        http::AsyncReq::CallbackType const nullOnFinish = nullptr;
        string const data = _requestJson.is_null() ? "" : _requestJson.dump();
        auto req = http::AsyncReq::create(ioService, nullOnFinish, _method, url, data, headerMap);
        req->setExpirationIval(_timeoutMs * 1000);
        req->start();
        req->wait();
        if (req->state() == http::AsyncReq::State::FINISHED && req->responseCode() == 200) {
            if (req->responseBodySize() > 0) {
                try {
                    responseJson = json::parse(req->responseBody());
                    success = true;
                } catch (exception const& ex) {
                    errorMsg = ex.what();
                    success = false;
                }
            }
        } else {
            errorMsg = req->errorMessage() + " (response code: " + to_string(req->responseCode()) + ")";
        }
        ioService.stop();
        for (auto& thread : serviceThreads) {
            if (thread.joinable()) thread.join();
        }
    } catch (exception const& ex) {
        errorMsg = ex.what();
    } catch (...) {
        errorMsg = "unknown exception while processing HTTP request";
    }
    if (!_canceled) {
        try {
            _onFinish(_id, success, errorMsg, responseJson);
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Exception in onFinish callback: " << ex.what() << ", request id=" << _id);
        } catch (...) {
            LOGS(_log, LOG_LVL_ERROR, "Unknown exception in onFinish callback, request id=" << _id);
        }
    }
}

}  // namespace lsst::qserv::replica
