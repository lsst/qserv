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

// System headers
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/AsyncTimer.h"
#include "replica/IngestRequest.h"
#include "replica/IngestRequestMgr.h"
#include "replica/IngestResourceMgrT.h"
#include "replica/TransactionContrib.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE IngestRequestMgr
#include "boost/test/included/unit_test.hpp"

using namespace std;
using namespace boost::unit_test;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(IngestResourceMgrTest) {
    LOGS_INFO("IngestResourceMgr BEGIN");

    // Instantiate the manager.
    shared_ptr<IngestResourceMgrT> resourceMgr;
    BOOST_REQUIRE_NO_THROW({ resourceMgr = IngestResourceMgrT::create(); });
    BOOST_CHECK(resourceMgr != nullptr);

    BOOST_REQUIRE_THROW({ resourceMgr->asyncProcLimit(string()); }, std::invalid_argument);
    BOOST_REQUIRE_THROW({ resourceMgr->setAsyncProcLimit(string(), 0); }, std::invalid_argument);
    BOOST_REQUIRE_THROW({ resourceMgr->setAsyncProcLimit(string(), 1); }, std::invalid_argument);

    // Check defaults
    string const database1 = "db1";
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database1), 0U); });
    string const database2 = "db2";
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database2), 0U); });

    // Check setting non-trivial limits. Check for cross-talks. There should be none.
    unsigned int const limit1 = 1U;
    BOOST_REQUIRE_NO_THROW({ resourceMgr->setAsyncProcLimit(database1, limit1); });
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database1), limit1);
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database2), 0U);

    unsigned int const limit2 = 2U;
    BOOST_REQUIRE_NO_THROW({ resourceMgr->setAsyncProcLimit(database2, limit2); });
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database1), limit1);
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database2), limit2);

    // Reset the limits
    BOOST_REQUIRE_NO_THROW({ resourceMgr->setAsyncProcLimit(database1, 0U); });
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database1), 0U);
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database2), limit2);

    BOOST_REQUIRE_NO_THROW({ resourceMgr->setAsyncProcLimit(database2, 0U); });
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database1), 0U);
    BOOST_CHECK_EQUAL(resourceMgr->asyncProcLimit(database2), 0U);

    LOGS_INFO("IngestResourceMgr END");
}

BOOST_AUTO_TEST_CASE(IngestRequestMgrSimpleTest) {
    LOGS_INFO("IngestRequestMgr_simple BEGIN");

    // Start a separate thread for handling BOOST ASIO events (including the ones
    // needed to operate the timer). The thread will be shared by many timers.
    boost::asio::io_service io_service;
    unique_ptr<thread> ioServiceThread(new thread([&io_service]() {
        boost::asio::io_service::work work(io_service);
        io_service.run();
    }));

    // Prepare the timer for abort the process in case if pulling requests
    // from the manager will get locked due to problems in the implementaton
    // of the manager. The timer will be fired before each such operation and
    // be cancelled after completing the one.
    chrono::milliseconds const expirationIvalMs(1000);
    auto const timer = AsyncTimer::create(io_service, expirationIvalMs, [](auto expirationIvalMs) {
        LOGS_INFO("IngestRequestMgr_simple: test exceeded the time budget of " << expirationIvalMs.count()
                                                                               << "ms");
        std::exit(1);
    });

    // Instantiate the manager.
    shared_ptr<IngestRequestMgr> mgr;
    BOOST_REQUIRE_NO_THROW({ mgr = IngestRequestMgr::test(); });
    BOOST_CHECK(mgr != nullptr);

    // Test if the queues are empty
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 0U);

    TransactionContribInfo inContrib1;
    inContrib1.id = 1;
    inContrib1.createTime = 100;
    inContrib1.status = TransactionContribInfo::Status::IN_PROGRESS;

    // Test if non-existing requests (provided by their unique identifiers)
    // are rejected by teh manager.
    BOOST_REQUIRE_THROW({ mgr->find(inContrib1.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ mgr->cancel(inContrib1.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ mgr->completed(inContrib1.id); }, IngestRequestNotFound);

    // Null objects can't be submitted.
    // The queues shall not be affected by this.
    BOOST_REQUIRE_THROW({ mgr->submit(nullptr); }, std::invalid_argument);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 0U);

    // Test submitting the first (and the only) request.
    shared_ptr<IngestRequest> inRequest1;
    BOOST_REQUIRE_NO_THROW({ inRequest1 = IngestRequest::test(inContrib1); });
    BOOST_CHECK(inRequest1 != nullptr);
    BOOST_REQUIRE_NO_THROW({ mgr->submit(inRequest1); });
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 0U);

    // The request shall be known to the manager. And request finder shall
    // not affect the queues.
    TransactionContribInfo outContrib1;
    BOOST_REQUIRE_NO_THROW({ outContrib1 = mgr->find(inContrib1.id); });
    BOOST_CHECK_EQUAL(outContrib1.id, inContrib1.id);
    BOOST_CHECK_EQUAL(outContrib1.createTime, inContrib1.createTime);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 0U);

    // Cancel the request while it's in the input queue.
    // The cancelled request shall move into the output queue.
    BOOST_REQUIRE_NO_THROW({ outContrib1 = mgr->cancel(inContrib1.id); });
    BOOST_CHECK_EQUAL(outContrib1.id, inContrib1.id);
    BOOST_CHECK_EQUAL(outContrib1.createTime, inContrib1.createTime);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 1U);

    // Register the second request.
    TransactionContribInfo inContrib2;
    inContrib2.id = 2;
    inContrib2.createTime = 200;
    // This status needs to be set explicitly here since no database support
    // is available for requests created for the unit tests. Normally this status
    // is set by the corresponding database services after succesfully registering
    // requests in the persistent state of the Replication system.
    // The default status TransactionContribInfo::Status::CREATED will be
    // regected by the manager by the method submit() called below.
    inContrib2.status = TransactionContribInfo::Status::IN_PROGRESS;

    BOOST_REQUIRE_THROW({ mgr->find(inContrib2.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ mgr->cancel(inContrib2.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ mgr->completed(inContrib2.id); }, IngestRequestNotFound);
    shared_ptr<IngestRequest> inRequest2;
    BOOST_REQUIRE_NO_THROW({ inRequest2 = IngestRequest::test(inContrib2); });
    BOOST_CHECK(inRequest2 != nullptr);
    BOOST_REQUIRE_NO_THROW({ mgr->submit(inRequest2); });
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 1U);

    // Pull the request for processing.
    // The request shall move from the input queue into the in-progress one.
    shared_ptr<IngestRequest> outRequest2;
    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest2 = mgr->next(); });
    timer->cancel();
    BOOST_CHECK(outRequest2 != nullptr);
    BOOST_CHECK_EQUAL(outRequest2->transactionContribInfo().id, inContrib2.id);
    BOOST_CHECK_EQUAL(outRequest2->transactionContribInfo().createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 1U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 1U);

    // Make sure any further attepts to pull requests from the empty input queue
    // will time out.
    BOOST_REQUIRE_THROW({ outRequest2 = mgr->next(expirationIvalMs); }, IngestRequestTimerExpired);

    // Cancel the request while it's in the in-progress queue.
    // The cancelled request will remain in the queue because of
    // the advisory cancellation.
    TransactionContribInfo outContrib2;
    BOOST_REQUIRE_NO_THROW({ outContrib2 = mgr->cancel(inContrib2.id); });
    BOOST_CHECK_EQUAL(outContrib2.id, inContrib2.id);
    BOOST_CHECK_EQUAL(outContrib2.createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 1U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 1U);

    // Notify the manager that the request processing has finished.
    // The request shall move from the in-progress queue into the output one.
    BOOST_REQUIRE_NO_THROW({ mgr->completed(inContrib2.id); });
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 2U);

    // Cancel the request while it's in the output queue.
    // The cancelled request will remain in the queue.
    BOOST_REQUIRE_NO_THROW({ outContrib2 = mgr->cancel(inContrib2.id); });
    BOOST_CHECK_EQUAL(outContrib2.id, inContrib2.id);
    BOOST_CHECK_EQUAL(outContrib2.createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(mgr->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(mgr->outputQueueSize(), 2U);

    // Cleanup the BOOST ASIO service to prevent the nasty SIGABRT of the process.
    io_service.stop();
    ioServiceThread->join();

    LOGS_INFO("IngestRequestMgr_simple END");
}

BOOST_AUTO_TEST_SUITE_END()
