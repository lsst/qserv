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
#ifndef LSST_QSERV_REPLICA_ASYNCTIMER_H
#define LSST_QSERV_REPLICA_ASYNCTIMER_H

// System headers
#include <chrono>
#include <functional>
#include <memory>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class AsyncTimer represents a simple asynchronous timer for initiating time-based
 * events within an application.
 *
 * The primary purpose of the class is for timing operations in the unit tests
 * and aborting tests in case of the lockups. The typical use of the timer is
 * illustrated below:
 * @code
 * // Set up the timer
 * boost::asio::io_service io_service;
 * std::chrono::milliseconds const expirationIvalMs(1000);
 * auto const timer = AsyncTimer::create(io_service, expirationIvalMs,
 *                                       [](std::chrono::milliseconds expirationIvalMs) {
 *     std::cerr << "The test locked up after " << expirationIvalMs.count() << "ms" << std::endl;
 *     std::abort();
 * });
 * timer->start();
 *
 * // Begin the test that may get locked up
 * ...
 *
 * // Cancel the timer, if the above initiated test finished within the desired
 * // time budget.
 * timer->cancel();
 * ...
 * @endcode
 */
class AsyncTimer : public std::enable_shared_from_this<AsyncTimer> {
public:
    /// The function type for notifications on the completion of the operation.
    /// The only parameter of the function is a value of the expiration interval.
    typedef std::function<void(std::chrono::milliseconds)> CallbackType;

    /**
     * The factory method.
     * @throws std::invalid_argument If the 0 interval or the null callback pointer
     *   is passed into the method.
     */
    static std::shared_ptr<AsyncTimer> create(boost::asio::io_service& io_service,
                                              std::chrono::milliseconds expirationIvalMs,
                                              CallbackType const& onFinish);

    AsyncTimer(AsyncTimer const&) = delete;
    AsyncTimer& operator=(AsyncTimer const&) = delete;

    /// Non-trivial destrictor is needed to cancel the deadline timer when the current
    /// object gets destroyed in the end of a code block, or when the application
    /// is exiting.
    ~AsyncTimer();

    /// Start (or restart of already running) the timer.
    /// If the timer gets restarted then it will begin counting again the interval
    /// specified in the class's constructor.
    void start();

    /// Cancel the timer.
    /// @return 'false' if the time expired or was already canceled.
    bool cancel();

private:
    AsyncTimer(boost::asio::io_service& io_service, std::chrono::milliseconds expirationIvalMs,
               CallbackType const& onFinish);

    /// The method gets called when the deadline timer gets expired or canceled
    /// If the timer was explicitly canceled then no user-provoded callback will
    /// get called.
    /// @param ec An error code to be anylzed to see if the timer expired, or if
    ///  it was cancelled by calling the class's method cancel().
    void _expired(boost::system::error_code const& ec);

    boost::asio::io_service& _io_service;
    std::chrono::milliseconds const _expirationIvalMs;
    CallbackType _onFinish;

    boost::asio::deadline_timer _timer;

    /// The mutex for enforcing thread safety of the class public API
    /// and internal operations.
    mutable replica::Mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_ASYNCTIMER_H
