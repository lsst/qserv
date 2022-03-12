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
 /**
  * @brief test AsyncTimer
  */

// System headers
#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/AsyncTimer.h"

// Boost unit test header
#define BOOST_TEST_MODULE AsyncTimer
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(AsyncTimer_BasicOperations) {
    LOGS_INFO("AsyncTimer_BasicOperations: started");

    // The timer object is trivially created, and it shouldn't be running upon creation.
    shared_ptr<AsyncTimer> timer;
    BOOST_REQUIRE_NO_THROW({
        timer = AsyncTimer::create();
    });
    BOOST_CHECK(timer != nullptr);
    BOOST_CHECK(!timer->isRunning());

    // Null interval or null callback aren't allowed.
    BOOST_CHECK_THROW({
        timer->start(0, []() {});
    }, std::invalid_argument);

    BOOST_CHECK_THROW({
        timer->start(1, nullptr);
    }, std::invalid_argument);

    unsigned int const expirationIvalMs = 500;
    atomic_bool expired{false};
    BOOST_REQUIRE_NO_THROW({
        timer->start(expirationIvalMs, [&expired]() { expired = true; });
    });
    BOOST_CHECK(timer->isRunning());

    // This should fail for as long as the timer is still running.
    BOOST_CHECK_THROW({
        timer->start(expirationIvalMs, []() {});
    }, std::logic_error);
    BOOST_CHECK(timer->isRunning());

    // Wait enough to ensure the timer got expired.
    this_thread::sleep_for(chrono::milliseconds(2 * expirationIvalMs));
    BOOST_CHECK(!timer->isRunning());
    BOOST_CHECK(expired);
}

BOOST_AUTO_TEST_CASE(AsyncTimer_Cancellation) {
    LOGS_INFO("AsyncTimer_Cancellation: started");

    shared_ptr<AsyncTimer> const timer = AsyncTimer::create();

    unsigned int const expirationIvalMs = 500;
    atomic_bool expired{false};
    BOOST_REQUIRE_NO_THROW({
        timer->start(expirationIvalMs, [&expired]() { expired = true; });
    });

    // Cancel after approximately the half way toward the timer expiration event
    this_thread::sleep_for(chrono::milliseconds(250));

    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(timer->cancel());
    });
    BOOST_CHECK(!timer->isRunning());
    BOOST_CHECK(!expired);
}

BOOST_AUTO_TEST_CASE(AsyncTimer_Reuse) {
    LOGS_INFO("AsyncTimer_Reuse: started");

    shared_ptr<AsyncTimer> const timer = AsyncTimer::create();

    unsigned int const expirationIvalMs = 250;
    for (int i = 0; i < 4; ++i) {
        LOGS_INFO("AsyncTimer_Reuse: iteration: " + to_string(i));
        atomic_bool expired{false};
        BOOST_REQUIRE_NO_THROW({
            timer->start(expirationIvalMs, [&expired]() { expired = true; });
        });
        BOOST_CHECK(timer->isRunning());

        // Wait enough to ensure the timer got expired.
        this_thread::sleep_for(chrono::milliseconds(2 * expirationIvalMs));
        BOOST_CHECK(!timer->isRunning());
        BOOST_CHECK(expired);
    }
}

BOOST_AUTO_TEST_SUITE_END()
