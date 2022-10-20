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
#include "replica/Performance.h"
#include "replica/TransactionContrib.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE IngestRequestMgr
#include "boost/test/included/unit_test.hpp"

using namespace std;
using namespace boost::unit_test;
using namespace lsst::qserv::replica;

namespace {

// The "external" constructor for objects of the class TransactionContribInfo
// that initializes attributes as required by the unit tests.
auto const makeContrib = [](unsigned int id, char const* databaseName) -> TransactionContribInfo {
    TransactionContribInfo contrib;
    contrib.id = id;
    contrib.createTime = PerformanceUtils::now();
    contrib.database = databaseName;
    // This status needs to be set explicitly here since no database support
    // is available for requests created for the unit tests. Normally this status
    // is set by the corresponding database services after succesfully registering
    // requests in the persistent state of the Replication system.
    // The default status TransactionContribInfo::Status::CREATED will be
    // rejected by the manager by the method submit() called below.
    contrib.status = TransactionContribInfo::Status::IN_PROGRESS;
    return contrib;
};
}  // namespace

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
    shared_ptr<IngestRequestMgr> requestScheduler;
    BOOST_REQUIRE_NO_THROW({ requestScheduler = IngestRequestMgr::test(); });
    BOOST_CHECK(requestScheduler != nullptr);

    // Test if the queues are empty
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    auto const inContrib1 = ::makeContrib(1, "db1");

    // Test if non-existing requests (provided by their unique identifiers)
    // are rejected by the manager.
    BOOST_REQUIRE_THROW({ requestScheduler->find(inContrib1.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ requestScheduler->cancel(inContrib1.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ requestScheduler->completed(inContrib1.id); }, IngestRequestNotFound);

    // Null objects can't be submitted.
    // The queues shall not be affected by this.
    BOOST_REQUIRE_THROW({ requestScheduler->submit(nullptr); }, std::invalid_argument);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // Test submitting the first (and the only) request.
    shared_ptr<IngestRequest> inRequest1;
    BOOST_REQUIRE_NO_THROW({ inRequest1 = IngestRequest::test(inContrib1); });
    BOOST_CHECK(inRequest1 != nullptr);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(inRequest1); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(inContrib1.database), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // The request shall be known to the manager. And request finder shall
    // not affect the queues.
    TransactionContribInfo outContrib1;
    BOOST_REQUIRE_NO_THROW({ outContrib1 = requestScheduler->find(inContrib1.id); });
    BOOST_CHECK_EQUAL(outContrib1.id, inContrib1.id);
    BOOST_CHECK_EQUAL(outContrib1.createTime, inContrib1.createTime);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(inContrib1.database), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // Cancel the request while it's in the input queue.
    // The cancelled request shall move into the output queue.
    BOOST_REQUIRE_NO_THROW({ outContrib1 = requestScheduler->cancel(inContrib1.id); });
    BOOST_CHECK_EQUAL(outContrib1.id, inContrib1.id);
    BOOST_CHECK_EQUAL(outContrib1.createTime, inContrib1.createTime);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Register the second request.
    auto const inContrib2 = ::makeContrib(2, "db1");
    BOOST_REQUIRE_THROW({ requestScheduler->find(inContrib2.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ requestScheduler->cancel(inContrib2.id); }, IngestRequestNotFound);
    BOOST_REQUIRE_THROW({ requestScheduler->completed(inContrib2.id); }, IngestRequestNotFound);
    shared_ptr<IngestRequest> inRequest2;
    BOOST_REQUIRE_NO_THROW({ inRequest2 = IngestRequest::test(inContrib2); });
    BOOST_CHECK(inRequest2 != nullptr);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(inRequest2); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(inContrib2.database), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Pull the request for processing.
    // The request shall move from the input queue into the in-progress one.
    shared_ptr<IngestRequest> outRequest2;
    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest2 = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK(outRequest2 != nullptr);
    BOOST_CHECK_EQUAL(outRequest2->transactionContribInfo().id, inContrib2.id);
    BOOST_CHECK_EQUAL(outRequest2->transactionContribInfo().createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(inContrib2.database), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Make sure any further attepts to pull requests from the empty input queue
    // will time out.
    BOOST_REQUIRE_THROW({ outRequest2 = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Cancel the request while it's in the in-progress queue.
    // The cancelled request will remain in the queue because of
    // the advisory cancellation.
    TransactionContribInfo outContrib2;
    BOOST_REQUIRE_NO_THROW({ outContrib2 = requestScheduler->cancel(inContrib2.id); });
    BOOST_CHECK_EQUAL(outContrib2.id, inContrib2.id);
    BOOST_CHECK_EQUAL(outContrib2.createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(inContrib2.database), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Notify the manager that the request processing has finished.
    // The request shall move from the in-progress queue into the output one.
    BOOST_REQUIRE_NO_THROW({ requestScheduler->completed(inContrib2.id); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 2U);

    // Cancel the request while it's in the output queue.
    // The cancelled request will remain in the queue.
    BOOST_REQUIRE_NO_THROW({ outContrib2 = requestScheduler->cancel(inContrib2.id); });
    BOOST_CHECK_EQUAL(outContrib2.id, inContrib2.id);
    BOOST_CHECK_EQUAL(outContrib2.createTime, inContrib2.createTime);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 2U);

    // Cleanup the BOOST ASIO service to prevent the nasty SIGABRT of the process.
    io_service.stop();
    ioServiceThread->join();

    LOGS_INFO("IngestRequestMgr_simple END");
}

BOOST_AUTO_TEST_CASE(IngestRequestMgrComplexTest) {
    LOGS_INFO("IngestRequestMgr_complex BEGIN");

    // ATTENTION: This test assumes that the basic operations with the scheduler
    // successfully passed the previous "simple" test. Hence, the main focus of
    // the current test is to see the effect of the request concurrency limits
    // set for databases.

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
    chrono::milliseconds const expirationIvalMs(10);
    auto const timer = AsyncTimer::create(io_service, expirationIvalMs, [](auto expirationIvalMs) {
        LOGS_INFO("IngestRequestMgr_simple: test exceeded the time budget of " << expirationIvalMs.count()
                                                                               << "ms");
        std::exit(1);
    });

    shared_ptr<IngestResourceMgrT> const resourceMgr = IngestResourceMgrT::create();
    shared_ptr<IngestRequestMgr> const requestScheduler = IngestRequestMgr::test(resourceMgr);

    // The concurrency limit of 2 set below for the database should be set
    // before registering requests. Otherwise, the limit won't be recognised.
    char const* db1 = "db1";
    resourceMgr->setAsyncProcLimit(db1, 2);

    // Submit 3 requests. Note that the creation time of each subsequent request
    // will be newer than the one of the previous request.
    auto const inContrib_1_of_db1 = ::makeContrib(1001, db1);
    auto const inContrib_2_of_db1 = ::makeContrib(1002, db1);
    auto const inContrib_3_of_db1 = ::makeContrib(1003, db1);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_1_of_db1)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_2_of_db1)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_3_of_db1)); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // Schedule two requests. The scheduler should not block the current thread.
    shared_ptr<IngestRequest> outRequest;
    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_1_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_2_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // This attempt should block since we're about to exceed the concurrency limit
    // specified for the database.
    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Submit 3 more requests in a scope of a different database.
    char const* db2 = "db2";
    auto const inContrib_1_of_db2 = ::makeContrib(2001, db2);
    auto const inContrib_2_of_db2 = ::makeContrib(2002, db2);
    auto const inContrib_3_of_db2 = ::makeContrib(2003, db2);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_1_of_db2)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_2_of_db2)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_3_of_db2)); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // Schedule three requests. The scheduler should not block the current thread.
    // All scheduled requests should belong to the second database that has no
    // resource limit.
    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_1_of_db2.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db2), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_2_of_db2.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db2), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_3_of_db2.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db2), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 0U);

    // This attempt should block since we've run out of the resource unconstrained
    // requests of the second database, and we would still exceed the concurrency limit
    // specified for the first database.
    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Now declare one in-progress request of tht first database
    // as completed. This should unblock us from scheduling the remaing
    // request of the database.
    BOOST_REQUIRE_NO_THROW({ requestScheduler->completed(inContrib_1_of_db1.id); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db2), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_3_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Submit more requests in a scope of the first database.
    auto const inContrib_4_of_db1 = ::makeContrib(1004, db1);
    auto const inContrib_5_of_db1 = ::makeContrib(1005, db1);
    auto const inContrib_6_of_db1 = ::makeContrib(1006, db1);
    auto const inContrib_7_of_db1 = ::makeContrib(1007, db1);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_4_of_db1)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_5_of_db1)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_6_of_db1)); });
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_7_of_db1)); });

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Furher increase concurrency limit of the first database. This should *NOT*
    // unblock the scheduler because the change gets into affect when
    // changes are made to the database's queue. The relevant events are:
    // submitting new requests, cancelling requests while they're still in
    // the input queue, finishing request processing.
    resourceMgr->setAsyncProcLimit(db1, 3);

    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 1U);

    // Test the above-mentioned condition by cancelling one of the input
    // requests of the first database.
    BOOST_REQUIRE_NO_THROW({ requestScheduler->cancel(inContrib_4_of_db1.id); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 2U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_5_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 6U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 2U);

    // Now we should be locked up since the number of in-progress requests
    // for the first database has reached the extended by 1 limit.
    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Mark one of those requests as completed and make another try.
    // The scheduler should let us move by one request only and then get
    // locked again.
    BOOST_REQUIRE_NO_THROW({ requestScheduler->completed(inContrib_2_of_db1.id); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 3U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_6_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 6U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 3U);

    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Extend the limit byy 2 and add one more request for the for database.
    // This will allow the scheduler to schedule 2 requests because the limits
    // are always being refreshed for the given database when new requests get
    // registered for the database.
    resourceMgr->setAsyncProcLimit(db1, 5);

    auto const inContrib_8_of_db1 = ::makeContrib(1008, db1);
    BOOST_REQUIRE_NO_THROW({ requestScheduler->submit(IngestRequest::test(inContrib_8_of_db1)); });
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 2U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 6U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 3U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_7_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(db1), 1U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 7U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 4U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 3U);

    timer->start();
    BOOST_REQUIRE_NO_THROW({ outRequest = requestScheduler->next(); });
    timer->cancel();
    BOOST_CHECK_EQUAL(outRequest->transactionContribInfo().id, inContrib_8_of_db1.id);

    BOOST_CHECK_EQUAL(requestScheduler->inputQueueSize(), 0U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(), 8U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db1), 5U);
    BOOST_CHECK_EQUAL(requestScheduler->inProgressQueueSize(db2), 3U);
    BOOST_CHECK_EQUAL(requestScheduler->outputQueueSize(), 3U);

    // No more input requests are available at this point.
    BOOST_REQUIRE_THROW({ outRequest = requestScheduler->next(expirationIvalMs); },
                        IngestRequestTimerExpired);

    // Cleanup the BOOST ASIO service to prevent the nasty SIGABRT of the process.
    io_service.stop();
    ioServiceThread->join();

    LOGS_INFO("IngestRequestMgr_complex END");
}

BOOST_AUTO_TEST_SUITE_END()
