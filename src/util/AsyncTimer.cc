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
#include "util/AsyncTimer.h"

// System headers
#include <stdexcept>

using namespace std;

namespace lsst::qserv::util {

shared_ptr<AsyncTimer> AsyncTimer::create(boost::asio::io_service& io_service,
                                          chrono::milliseconds expirationIvalMs,
                                          CallbackType const& onFinish) {
    return shared_ptr<AsyncTimer>(new AsyncTimer(io_service, expirationIvalMs, onFinish));
}

AsyncTimer::AsyncTimer(boost::asio::io_service& io_service, chrono::milliseconds expirationIvalMs,
                       CallbackType const& onFinish)
        : _io_service(io_service),
          _expirationIvalMs(expirationIvalMs),
          _onFinish(onFinish),
          _timer(_io_service) {
    string const context = "AsyncTimer::" + string(__func__) + " ";
    if (expirationIvalMs.count() == 0) throw invalid_argument(context + "0 interval is not allowed.");
    if (_onFinish == nullptr) throw invalid_argument(context + "null callback pointer is not allowed.");
}

AsyncTimer::~AsyncTimer() {
    _onFinish = nullptr;
    boost::system::error_code ec;
    _timer.cancel(ec);
}

bool AsyncTimer::start() {
    lock_guard<mutex> lock(_mtx);
    if (_onFinish == nullptr) return false;
    _timer.expires_from_now(boost::posix_time::milliseconds(_expirationIvalMs.count()));
    _timer.async_wait(
            [self = shared_from_this()](boost::system::error_code const& ec) { self->_expired(ec); });
    return true;
}

bool AsyncTimer::cancel() {
    lock_guard<mutex> lock(_mtx);
    if (nullptr == _onFinish) return false;
    _onFinish = nullptr;
    _timer.cancel();
    return true;
}

void AsyncTimer::_expired(boost::system::error_code const& ec) {
    if (ec == boost::asio::error::operation_aborted) return;
    CallbackType onFinish;
    {
        lock_guard<mutex> lock(_mtx);
        onFinish = _onFinish;
    }
    if (onFinish != nullptr) {
        bool const restart = onFinish(_expirationIvalMs);
        if (restart) start();
    }
}

}  // namespace lsst::qserv::util
