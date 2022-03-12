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
#ifndef LSST_QSERV_ASYNCTIMER_H
#define LSST_QSERV_ASYNCTIMER_H

// System headers
#include <functional>
#include <memory>

// Third-party headers
#include "boost/asio.hpp"

// Qserv headers
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * @brief Class AsyncTimer represents a simple asynchronous timer for
 *   initiating time-based events within an application.
 * 
 * The implementation of the class invokes a user-supplied callback (lambda) function
 * upon the expiration of the specified timeout. The timer is started by method start(),
 * and it's stopped by method cancel(). If method 'cancel()' is called before the timeout
 * expiration event then the callback function won't be called.
 *
 * The same timer object can be reused many times if needed. The only requirement is that
 * the previous interval had to expire or be explicitly stopped before attempting another
 * one.
 *
 * Here is an example of using the class to abort the process after 5 seconds.
 * @code
 *   unsigned int const expirationIvalMs = 500;
 *   AsyncTimer processAbortTimer;
 *   timer.start(
 *       expirationIvalMs,
 *       []() {
 *           std::abort();
 *       });
 * @code
 * @note The implementation of the timer API is thread-safe.
 * @note The implementation starts one thread for managing the timer. The thread exists
 *   for the duration of the timer before it gets expired or gets explicitly cancelled.
 */
class AsyncTimer: public std::enable_shared_from_this<AsyncTimer>  {
public:
    /// The function type for notifications on the completion of the operation.
    typedef std::function<void()> CallbackType;

    /**
     * @brief The factory method for creating timers.
     * @return The shared pointer to the newely created object.
     */
    static std::shared_ptr<AsyncTimer> create() {
        return std::shared_ptr<AsyncTimer>(new AsyncTimer());
    }

    AsyncTimer(AsyncTimer const&) = delete;
    AsyncTimer& operator=(AsyncTimer const&) = delete;

    /// Non-trivial destructor is needed to free up allocated resources.
    virtual ~AsyncTimer();

    /**
     * @brief Start the timer.
     * 
     * @param expirationIvalMs A timeout (milliseconds) to wait before expiring the timer.
     *   Must be greater than 0.
     * @param onFinish The callback function to be called upon the expiration
     *   of the specified timeout. Must not be nullptr.
     * @throw std::logic_error If the timer was already started.
     * @throw std::invalid_argument For invalid values of the input parameters.
     */
    void start(unsigned int expirationIvalMs,
               CallbackType const& onFinish);

    /**
     * @brief Cancel (stop) the timer.
     * @return 'true' if the timer was cancelled.
     */
    bool cancel();

    /// @return 'true' if the timer is running.
    bool isRunning() const;

private:
    AsyncTimer();

    void _expired(boost::system::error_code const& ec);

    CallbackType _onFinish = nullptr;

    boost::asio::io_service _io_service;
    boost::asio::deadline_timer _timer;

    /// The mutex for enforcing thread safety of the class public API
    /// and internal operations.
    mutable util::Mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_ASYNCTIMER_H
