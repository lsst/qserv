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
#include "http/AsyncReq.h"

// Standard headers
#include <algorithm>
#include <stdexcept>
#include <thread>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "http/Exceptions.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace asio = boost::asio;
namespace http = boost::beast::http;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.http.AsyncReq");

boost::beast::http::verb method2verb(string const& method) {
    if (method == "GET")
        return boost::beast::http::verb::get;
    else if (method == "POST")
        return boost::beast::http::verb::post;
    else if (method == "PUT")
        return boost::beast::http::verb::put;
    else if (method == "DELETE")
        return boost::beast::http::verb::delete_;
    throw invalid_argument("AsyncReq::" + string(__func__) + " invalid method '" + method + "'.");
}
}  // namespace

namespace lsst::qserv::http {

string AsyncReq::state2str(State state) {
    switch (state) {
        case State::CREATED:
            return "CREATED";
        case State::IN_PROGRESS:
            return "IN_PROGRESS";
        case State::FINISHED:
            return "FINISHED";
        case State::FAILED:
            return "FAILED";
        case State::BODY_LIMIT_ERROR:
            return "BODY_LIMIT_ERROR";
        case State::CANCELLED:
            return "CANCELLED";
        case State::EXPIRED:
            return "EXPIRED";
    }
    throw invalid_argument("AsyncReq::" + string(__func__) +
                           " unknown state: " + to_string(static_cast<int>(state)) + ".");
}

shared_ptr<AsyncReq> AsyncReq::create(asio::io_service& io_service, CallbackType const& onFinish,
                                      string const& method, string const& url, string const& data,
                                      std::unordered_map<std::string, std::string> const& headers,
                                      size_t maxResponseBodySize, unsigned int expirationIvalSec) {
    return shared_ptr<AsyncReq>(new AsyncReq(io_service, onFinish, method, url, data, headers,
                                             maxResponseBodySize, expirationIvalSec));
}

AsyncReq::AsyncReq(asio::io_service& io_service, CallbackType const& onFinish, string const& method,
                   string const& url, string const& data,
                   std::unordered_map<std::string, std::string> const& headers, size_t maxResponseBodySize,
                   unsigned int expirationIvalSec)
        : _io_service(io_service),
          _resolver(io_service),
          _socket(io_service),
          _onFinish(onFinish),
          _method(method),
          _url(url),
          _data(data),
          _headers(headers),
          _maxResponseBodySize(maxResponseBodySize),
          _expirationIvalSec(expirationIvalSec),
          _expirationTimer(io_service),
          _timer(io_service) {
    string const context = "AsyncReq::" + string(__func__) + " ";
    if (_url.scheme() != Url::Scheme::HTTP) {
        throw invalid_argument(context + "this implementation only supports urls based on the HTTP scheme.");
    }

    // Prepare the request
    _req.version(11);  // HTTP/1.1
    _req.method(::method2verb(_method));
    _req.target(_url.target());
    _req.set(boost::beast::http::field::host, _url.host());
    _req.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    _req.body() = _data;
    _req.content_length(_data.size());
    auto& header = _req.base();
    for (auto&& itr : _headers) {
        header.set(itr.first, itr.second);
    }
}

AsyncReq::~AsyncReq() {
    _expirationTimer.cancel();
    _timer.cancel();
    boost::system::error_code ec;
    _socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
}

string AsyncReq::version() const {
    switch (_req.version()) {
        case 11:
            return "HTTP/1.1";
        default:
            return "HTTP/?";
    }
}

void AsyncReq::start() {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    _assertState(lock, context, {State::CREATED});
    try {
        _resolve(lock);
        if (_expirationIvalSec) {
            _expirationTimer.expires_from_now(boost::posix_time::seconds(_expirationIvalSec));
            _expirationTimer.async_wait(
                    [self = shared_from_this()](boost::system::error_code const& ec) { self->_expired(ec); });
        }
        _state = State::IN_PROGRESS;
    } catch (exception const& ex) {
        _finish(lock, State::FAILED, ex.what());
        throw;
    }
}

bool AsyncReq::cancel() {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    switch (_state) {
        case State::CREATED:
        case State::IN_PROGRESS:
            _finish(lock, State::CANCELLED);
            return true;
        default:
            return false;
    }
}

void AsyncReq::wait() {
    if ((State::CREATED != _state) && (State::IN_PROGRESS != _state)) return;
    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _onFinishCv.wait(onFinishLock, [&] { return _finished.load(); });
}

string AsyncReq::errorMessage() const {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    return _error;
}

int AsyncReq::responseCode() const {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    _assertState(lock, context, {State::FINISHED, State::BODY_LIMIT_ERROR});
    auto const& header = _res.get().base();
    return header.result_int();
}

unordered_map<string, string> const& AsyncReq::responseHeader() const {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    _assertState(lock, context, {State::FINISHED, State::BODY_LIMIT_ERROR});
    return _responseHeader;
}

string const& AsyncReq::responseBody() const {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    _assertState(lock, context, {State::FINISHED});
    return _res.get().body();
}

size_t AsyncReq::responseBodySize() const {
    string const context = "AsyncReq::" + string(__func__) + " ";
    std::lock_guard<std::mutex> const lock(_mtx);
    _assertState(lock, context, {State::FINISHED});
    return _res.get().body().size();
}

void AsyncReq::_restart(std::lock_guard<std::mutex> const& lock) {
    _timer.cancel();
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
            [self = shared_from_this()](boost::system::error_code const& ec) { self->_restarted(ec); });
}

void AsyncReq::_restarted(boost::system::error_code const& ec) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    if (State::IN_PROGRESS != _state) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (State::IN_PROGRESS != _state) return;

    _resolve(lock);
}

void AsyncReq::_resolve(std::lock_guard<std::mutex> const& lock) {
    _resolver.async_resolve(
            _url.host(), to_string(_url.port() == 0 ? 80 : _url.port()),
            [self = shared_from_this()](boost::system::error_code const& ec,
                                        asio::ip::tcp::resolver::results_type const& results) {
                self->_resolved(ec, results);
            });
}

void AsyncReq::_resolved(boost::system::error_code const& ec,
                         asio::ip::tcp::resolver::results_type const& results) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    if (State::IN_PROGRESS != _state) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (State::IN_PROGRESS != _state) return;

    if (ec.value() != 0) {
        _logError(context + "failed to resolve the host/port", ec);
        _restart(lock);
        return;
    }
    asio::async_connect(
            _socket, results,
            [self = shared_from_this()](boost::system::error_code const& ec,
                                        const asio::ip::tcp::endpoint& endpoint) { self->_connected(ec); });
}

void AsyncReq::_connected(boost::system::error_code const& ec) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    if (State::IN_PROGRESS != _state) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (State::IN_PROGRESS != _state) return;

    if (ec.value() != 0) {
        _logError(context + "failed to connect to the server", ec);
        _restart(lock);
        return;
    }
    boost::beast::http::async_write(
            _socket, _req,
            [self = shared_from_this()](boost::system::error_code const& ec, size_t bytesSent) {
                self->_sent(ec, bytesSent);
            });
}

void AsyncReq::_sent(boost::system::error_code const& ec, size_t bytesSent) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    if (State::IN_PROGRESS != _state) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (State::IN_PROGRESS != _state) return;

    if (ec.value() != 0) {
        _logError(context + "failed to send a request", ec);
        _restart(lock);
        return;
    }
    if (_maxResponseBodySize != 0) {
        _res.body_limit(_maxResponseBodySize);
    }
    boost::beast::http::async_read(
            _socket, _buffer, _res,
            [self = shared_from_this()](boost::system::error_code const& ec, size_t bytesReceived) {
                self->_received(ec, bytesReceived);
            });
}

void AsyncReq::_received(boost::system::error_code const& ec, size_t bytesReceived) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    if (_state != State::IN_PROGRESS) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (_state != State::IN_PROGRESS) return;

    State newState = State::FINISHED;

    if (ec.value() != 0) {
        if (ec == boost::beast::http::error::body_limit) {
            newState = State::BODY_LIMIT_ERROR;
        } else {
            _logError(context + "failed to receive server response", ec);
            _finish(lock, State::FAILED,
                    context + "failed to receive server response, ec: " + to_string(ec.value()) + " [" +
                            ec.message() + "]");
            return;
        }
    }
    // Response header will be valid in both states: FINISHED and BODY_LIMIT_ERROR.
    _extractCacheHeader(lock);
    _finish(lock, newState);
}

void AsyncReq::_extractCacheHeader(std::lock_guard<std::mutex> const& lock) {
    auto const& header = _res.get().base();
    for (auto itr = header.cbegin(); itr != header.cend(); ++itr) {
        _responseHeader.insert(pair<string, string>(itr->name_string(), itr->value()));
    }
}

void AsyncReq::_expired(boost::system::error_code const& ec) {
    string const context = "AsyncReq::" + string(__func__) + " ";

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    if (_state != State::IN_PROGRESS) return;
    std::lock_guard<std::mutex> const lock(_mtx);
    if (_state != State::IN_PROGRESS) return;

    _finish(lock, State::EXPIRED);
}

void AsyncReq::_finish(std::lock_guard<std::mutex> const& lock, State finalState, string const& error) {
    _state = finalState;
    _error = error;

    // Stop the timers if any is is still running
    _expirationTimer.cancel();
    _timer.cancel();

    // This will cancel any outstanding network operations
    boost::system::error_code ec;
    _socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);

    if (nullptr != _onFinish) {
        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure
        _io_service.post([self = shared_from_this(), onFinish = move(_onFinish)] { onFinish(self); });
        _onFinish = nullptr;
    }

    // Unblock a caller that might be blocked while explicitly waiting for
    // the completion of the request.
    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _finished = true;
    _onFinishCv.notify_one();
}

void AsyncReq::_assertState(std::lock_guard<std::mutex> const& lock, string const& context,
                            initializer_list<State> const& desiredStates) const {
    if (find(desiredStates.begin(), desiredStates.end(), _state) == desiredStates.end()) {
        string states;
        for (auto&& state : desiredStates) {
            if (!states.empty()) states += ",";
            states += state2str(state);
        }
        throw logic_error(context + "none of the desired states in [" + states +
                          "] matches the current state " + state2str(_state));
    }
}

void AsyncReq::_logError(string const& prefix, boost::system::error_code const& ec) const {
    LOGS(_log, LOG_LVL_WARN,
         prefix << " method: " << _method << " url: " << _url.url() << " host: " << _url.host()
                << " port: " << _url.port() << " target: " << _url.target() << " ec: " << ec.value() << " ["
                << ec.message() << "]");
}

}  // namespace lsst::qserv::http
