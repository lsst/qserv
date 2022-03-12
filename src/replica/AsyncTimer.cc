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
#include "replica/AsyncTimer.h"

// Standard headers
#include <stdexcept>
#include <string>
#include <thread>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

using namespace std;
namespace asio = boost::asio;

namespace lsst {
namespace qserv {
namespace replica {

AsyncTimer::AsyncTimer()
    :   _io_service(),
        _timer(_io_service) {
}


AsyncTimer::~AsyncTimer() {
    _timer.cancel();
}


void AsyncTimer::start(unsigned int expirationIvalMs, CallbackType const& onFinish) {
    string const context = "AsyncTimer::" + string(__func__) + " ";
    util::Lock lock(_mtx, context);
    if (nullptr != _onFinish) throw logic_error(context + " timer is already running.");
    if (nullptr == onFinish) throw invalid_argument(context + " the callback function can't be nullptr.");
    if (0 == expirationIvalMs) throw invalid_argument(context + " the expiration interval must be greater than 0.");
    _onFinish = onFinish;
    auto const self = shared_from_this();
    // The timer needs to be registered first to prevent premature completion
    // of _io_service that will be started in the separate thread.
    _timer.cancel();
    _timer.expires_from_now(boost::posix_time::milliseconds(expirationIvalMs));
    _timer.async_wait([self] (boost::system::error_code const& ec) {
        self->_expired(ec);
    });
    // The thread will automatically end when the timer will expire or be cancelled.
    // Note that restarting I/O service is needed for reusing this object. Otherwise
    // subsequent attempts to start the timer won't work (the timer will never fire
    // the expiration event).
    thread t([self] () {
        self->_io_service.restart();
        self->_io_service.run();
    });
    t.detach();
}


bool AsyncTimer::cancel() {
    string const context = "AsyncTimer::" + string(__func__) + " ";
    util::Lock lock(_mtx, context);
    if (nullptr == _onFinish) return false;
    _onFinish = nullptr;
    _timer.cancel();
    return true;
}


bool AsyncTimer::isRunning() const {
    string const context = "AsyncTimer::" + string(__func__) + " ";
    util::Lock lock(_mtx, context);
    return nullptr != _onFinish;
}


void AsyncTimer::_expired(boost::system::error_code const& ec) {
    string const context = "AsyncTimer::" + string(__func__) + " ";
    util::Lock lock(_mtx, context);
    if (ec == boost::asio::error::operation_aborted) return;
    if (nullptr == _onFinish) return;
    _onFinish();
    _onFinish = nullptr;
}

}}} // namespace lsst::qserv::replica
