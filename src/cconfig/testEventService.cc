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
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/DataManagementEvent.h"
#include "cconfig/EventService.h"

// Boost unit test header
#define BOOST_TEST_MODULE EventService
#include <boost/test/unit_test.hpp>

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::cconfig;

BOOST_AUTO_TEST_SUITE(Suite)

// ============================================================================
// DataManagementEvent Tests
// ============================================================================

BOOST_AUTO_TEST_CASE(DataManagementEventTypeConversions) {
    LOGS_INFO("DataManagementEvent type conversion test begins");

    // Test all type conversions
    BOOST_CHECK_EQUAL(DataManagementEvent::type2str(DataManagementEvent::Type::NONE), "NONE");
    BOOST_CHECK_EQUAL(DataManagementEvent::type2str(DataManagementEvent::Type::CHUNK_MAP_REBUILT),
                      "CHUNK_MAP_REBUILT");
    BOOST_CHECK_EQUAL(DataManagementEvent::type2str(DataManagementEvent::Type::DATABASE_PUBLISHED),
                      "DATABASE_PUBLISHED");
    BOOST_CHECK_EQUAL(DataManagementEvent::type2str(DataManagementEvent::Type::DATABASE_DELETED),
                      "DATABASE_DELETED");
    BOOST_CHECK_EQUAL(DataManagementEvent::type2str(DataManagementEvent::Type::TABLE_DELETED),
                      "TABLE_DELETED");

    // Test reverse conversions
    BOOST_CHECK(DataManagementEvent::str2type("NONE") == DataManagementEvent::Type::NONE);
    BOOST_CHECK(DataManagementEvent::str2type("CHUNK_MAP_REBUILT") ==
                DataManagementEvent::Type::CHUNK_MAP_REBUILT);
    BOOST_CHECK(DataManagementEvent::str2type("DATABASE_PUBLISHED") ==
                DataManagementEvent::Type::DATABASE_PUBLISHED);
    BOOST_CHECK(DataManagementEvent::str2type("DATABASE_DELETED") ==
                DataManagementEvent::Type::DATABASE_DELETED);
    BOOST_CHECK(DataManagementEvent::str2type("TABLE_DELETED") == DataManagementEvent::Type::TABLE_DELETED);

    // Test invalid type conversion
    BOOST_CHECK_THROW(DataManagementEvent::str2type("INVALID_TYPE"), invalid_argument);

    LOGS_INFO("DataManagementEvent type conversion test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventJsonSerialization) {
    LOGS_INFO("DataManagementEvent JSON serialization test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    DataManagementEvent event;
    event.type = DataManagementEvent::Type::DATABASE_PUBLISHED;
    event.database = "test_db";
    event.timestamp = now;

    json j = event.toJson();
    BOOST_CHECK_EQUAL(j.at("type").get<string>(), "DATABASE_PUBLISHED");
    BOOST_CHECK_EQUAL(j.at("database").get<string>(), "test_db");
    BOOST_CHECK_EQUAL(j.at("timestamp").get<long long>(), nowMs);
    BOOST_CHECK_EQUAL(j.at("table").get<string>(), "");

    LOGS_INFO("DataManagementEvent JSON serialization test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonNone) {
    LOGS_INFO("DataManagementEvent fromJson NONE type test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    json j = json::object({{"type", "NONE"}, {"timestamp", nowMs}});

    DataManagementEvent event = DataManagementEvent::fromJson(j);
    BOOST_CHECK(event.type == DataManagementEvent::Type::NONE);
    BOOST_CHECK_EQUAL(event.database, "");
    BOOST_CHECK_EQUAL(event.table, "");

    LOGS_INFO("DataManagementEvent fromJson NONE type test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonChunkMapRebuilt) {
    LOGS_INFO("DataManagementEvent fromJson CHUNK_MAP_REBUILT type test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    json j = json::object({{"type", "CHUNK_MAP_REBUILT"}, {"timestamp", nowMs}});

    DataManagementEvent event = DataManagementEvent::fromJson(j);
    BOOST_CHECK(event.type == DataManagementEvent::Type::CHUNK_MAP_REBUILT);
    BOOST_CHECK_EQUAL(event.database, "");
    BOOST_CHECK_EQUAL(event.table, "");

    LOGS_INFO("DataManagementEvent fromJson CHUNK_MAP_REBUILT type test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonDatabasePublished) {
    LOGS_INFO("DataManagementEvent fromJson DATABASE_PUBLISHED type test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    json j = json::object({{"type", "DATABASE_PUBLISHED"}, {"timestamp", nowMs}, {"database", "my_db"}});

    DataManagementEvent event = DataManagementEvent::fromJson(j);
    BOOST_CHECK(event.type == DataManagementEvent::Type::DATABASE_PUBLISHED);
    BOOST_CHECK_EQUAL(event.database, "my_db");
    BOOST_CHECK_EQUAL(event.table, "");

    LOGS_INFO("DataManagementEvent fromJson DATABASE_PUBLISHED type test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonDatabaseDeleted) {
    LOGS_INFO("DataManagementEvent fromJson DATABASE_DELETED type test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    json j = json::object({{"type", "DATABASE_DELETED"}, {"timestamp", nowMs}, {"database", "old_db"}});

    DataManagementEvent event = DataManagementEvent::fromJson(j);
    BOOST_CHECK(event.type == DataManagementEvent::Type::DATABASE_DELETED);
    BOOST_CHECK_EQUAL(event.database, "old_db");
    BOOST_CHECK_EQUAL(event.table, "");

    LOGS_INFO("DataManagementEvent fromJson DATABASE_DELETED type test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonTableDeleted) {
    LOGS_INFO("DataManagementEvent fromJson TABLE_DELETED type test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    json j = json::object({{"type", "TABLE_DELETED"},
                           {"timestamp", nowMs},
                           {"database", "test_db"},
                           {"table", "test_table"}});

    DataManagementEvent event = DataManagementEvent::fromJson(j);
    BOOST_CHECK(event.type == DataManagementEvent::Type::TABLE_DELETED);
    BOOST_CHECK_EQUAL(event.database, "test_db");
    BOOST_CHECK_EQUAL(event.table, "test_table");

    LOGS_INFO("DataManagementEvent fromJson TABLE_DELETED type test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventFromJsonErrors) {
    LOGS_INFO("DataManagementEvent fromJson error cases test begins");

    auto now = chrono::system_clock::now();
    auto nowMs = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

    // Missing required timestamp
    json jMissingTimestamp = json::object({{"type", "NONE"}});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jMissingTimestamp), invalid_argument);

    // Missing required type
    json jMissingType = json::object({{"timestamp", nowMs}});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jMissingType), invalid_argument);

    // Invalid type
    json jInvalidType = json::object({{"type", "INVALID"}, {"timestamp", nowMs}});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jInvalidType), invalid_argument);

    // DATABASE_PUBLISHED missing required database field
    json jMissingDb = json::object({{"type", "DATABASE_PUBLISHED"}, {"timestamp", nowMs}});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jMissingDb), invalid_argument);

    // TABLE_DELETED missing required table field
    json jMissingTable =
            json::object({{"type", "TABLE_DELETED"}, {"timestamp", nowMs}, {"database", "test_db"}});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jMissingTable), invalid_argument);

    // Not a JSON object
    json jArray = json::array({1, 2, 3});
    BOOST_CHECK_THROW(DataManagementEvent::fromJson(jArray), invalid_argument);

    LOGS_INFO("DataManagementEvent fromJson error cases test ends");
}

BOOST_AUTO_TEST_CASE(DataManagementEventRoundTripJson) {
    LOGS_INFO("DataManagementEvent round-trip JSON test begins");

    DataManagementEvent originalEvent;
    originalEvent.type = DataManagementEvent::Type::TABLE_DELETED;
    originalEvent.database = "my_database";
    originalEvent.table = "my_table";
    originalEvent.timestamp = chrono::system_clock::now();

    // Convert to JSON and back
    json j = originalEvent.toJson();
    DataManagementEvent roundTripEvent = DataManagementEvent::fromJson(j);

    BOOST_CHECK(roundTripEvent.type == originalEvent.type);
    BOOST_CHECK_EQUAL(roundTripEvent.database, originalEvent.database);
    BOOST_CHECK_EQUAL(roundTripEvent.table, originalEvent.table);

    // Timestamps should be close (within millisecond precision)
    auto tsDiff = abs(
            chrono::duration_cast<chrono::milliseconds>(roundTripEvent.timestamp - originalEvent.timestamp)
                    .count());
    BOOST_CHECK_LT(tsDiff, 10);  // Allow 10ms difference due to precision loss

    LOGS_INFO("DataManagementEvent round-trip JSON test ends");
}

// ============================================================================
// EventService Tests
// ============================================================================

BOOST_AUTO_TEST_CASE(EventServiceCreation) {
    LOGS_INFO("EventService creation test begins");

    // Test valid creation
    auto service = EventService::create(1);
    BOOST_CHECK(service != nullptr);

    auto service4 = EventService::create(4);
    BOOST_CHECK(service4 != nullptr);

    // Test invalid creation (zero threads)
    BOOST_CHECK_THROW(EventService::create(0), invalid_argument);

    LOGS_INFO("EventService creation test ends");
}

BOOST_AUTO_TEST_CASE(EventServiceLifecycle) {
    LOGS_INFO("EventService lifecycle test begins");

    auto service = EventService::create(2);
    BOOST_CHECK(service != nullptr);
    BOOST_CHECK(!service->isRunning());

    // Start the service
    service->start();
    BOOST_CHECK(service->isRunning());

    // Try to start again (should throw)
    BOOST_CHECK_THROW(service->start(), EventServiceException);

    // Stop the service
    service->stop();
    BOOST_CHECK(!service->isRunning());

    // Can start again after stop
    service->start();
    BOOST_CHECK(service->isRunning());
    service->stop();
    BOOST_CHECK(!service->isRunning());

    // Try to stop again (should throw)
    BOOST_CHECK_THROW(service->stop(), EventServiceException);
    BOOST_CHECK(!service->isRunning());

    LOGS_INFO("EventService lifecycle test ends");
}

BOOST_AUTO_TEST_CASE(EventServiceSubscription) {
    LOGS_INFO("EventService subscription test begins");

    auto service = EventService::create(1);
    service->start();

    int callCount = 0;
    auto callback1 = [&callCount](DataManagementEvent const& event) { callCount++; };

    // Subscribe with empty ID (should throw)
    BOOST_CHECK_THROW(service->subscribe(callback1, ""), invalid_argument);

    // Subscribe
    service->subscribe(callback1, "sub1");

    // Try to subscribe with same ID (should throw)
    BOOST_CHECK_THROW(service->subscribe(callback1, "sub1"), EventServiceException);

    // Subscribe with different ID (should work)
    service->subscribe(callback1, "sub2");

    // Unsubscribe with empty ID (should throw)
    BOOST_CHECK_THROW(service->unsubscribe(""), invalid_argument);

    // Unsubscribe
    service->unsubscribe("sub1");

    // Try to unsubscribe non-existent subscription (should throw)
    BOOST_CHECK_THROW(service->unsubscribe("sub1"), EventServiceException);

    service->stop();

    LOGS_INFO("EventService subscription test ends");
}

BOOST_AUTO_TEST_CASE(EventServiceEventDelivery) {
    LOGS_INFO("EventService event delivery test begins");

    auto service = EventService::create(2);
    service->start();

    atomic<int> receivedCount{0};
    string receivedEventType;
    string receivedDatabase;

    auto callback = [&receivedCount, &receivedEventType,
                     &receivedDatabase](DataManagementEvent const& event) {
        receivedEventType = DataManagementEvent::type2str(event.type);
        receivedDatabase = event.database;
        receivedCount++;
    };

    service->subscribe(callback, "sub1");

    DataManagementEvent event;
    event.type = DataManagementEvent::Type::DATABASE_PUBLISHED;
    event.database = "test_db";

    int const numEvents = 1;
    service->postEvent(event);

    // Give async callback time to execute
    for (int i = 0; receivedCount < numEvents && i < 100; ++i) {
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    BOOST_CHECK_EQUAL(receivedCount, numEvents);
    BOOST_CHECK_EQUAL(receivedEventType, "DATABASE_PUBLISHED");
    BOOST_CHECK_EQUAL(receivedDatabase, "test_db");

    service->stop();

    LOGS_INFO("EventService event delivery test ends");
}

BOOST_AUTO_TEST_CASE(EventServiceMultipleSubscriptions) {
    LOGS_INFO("EventService multiple subscriptions test begins");

    auto service = EventService::create(2);
    service->start();

    atomic<int> count1{0};
    atomic<int> count2{0};

    auto callback1 = [&count1](DataManagementEvent const& event) { count1++; };
    auto callback2 = [&count2](DataManagementEvent const& event) { count2++; };

    service->subscribe(callback1, "sub1");
    service->subscribe(callback2, "sub2");

    DataManagementEvent event;
    event.type = DataManagementEvent::Type::CHUNK_MAP_REBUILT;

    service->postEvent(event);

    // Give async callbacks time to execute
    this_thread::sleep_for(chrono::milliseconds(100));

    BOOST_CHECK_EQUAL(count1, 1);
    BOOST_CHECK_EQUAL(count2, 1);

    service->unsubscribe("sub1");
    service->postEvent(event);

    this_thread::sleep_for(chrono::milliseconds(100));

    // count1 should still be 1 (no new event), count2 should be 2
    BOOST_CHECK_EQUAL(count1, 1);
    BOOST_CHECK_EQUAL(count2, 2);

    service->stop();

    LOGS_INFO("EventService multiple subscriptions test ends");
}

BOOST_AUTO_TEST_CASE(EventServicePostEventNotRunning) {
    LOGS_INFO("EventService post event when not running test begins");

    auto service = EventService::create(1);

    // Post event without starting service (should be silently ignored)
    DataManagementEvent event;
    event.type = DataManagementEvent::Type::DATABASE_PUBLISHED;
    service->postEvent(event);  // Should not throw

    LOGS_INFO("EventService post event when not running test ends");
}

BOOST_AUTO_TEST_CASE(EventServiceMultipleEvents) {
    LOGS_INFO("EventService multiple events test begins");

    auto service = EventService::create(4);
    service->start();

    atomic<int> receivedCount{0};

    auto callback = [&receivedCount](DataManagementEvent const& event) { receivedCount++; };

    service->subscribe(callback, "sub1");

    // Post multiple events
    int const numEvents = 10;
    for (int i = 0; i < numEvents; ++i) {
        DataManagementEvent event;
        event.type = DataManagementEvent::Type::DATABASE_PUBLISHED;
        event.database = "db_" + to_string(i);
        service->postEvent(event);
    }

    // Give async callbacks time to execute
    for (int i = 0; receivedCount < numEvents && i < 100; ++i) {
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    BOOST_CHECK_EQUAL(receivedCount, numEvents);

    service->stop();

    LOGS_INFO("EventService multiple events test ends");
}

BOOST_AUTO_TEST_SUITE_END()
