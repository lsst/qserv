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
#include "mysql/CsvBuffer.h"

// System headers
#include <string>
#include <stdexcept>
#include <thread>

// Boost unit test header
#define BOOST_TEST_MODULE CsvBuffer_1
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;

struct Fixture {
    Fixture(void) {}
    ~Fixture(void) {}
};

BOOST_FIXTURE_TEST_SUITE(suite, Fixture)

BOOST_AUTO_TEST_CASE(TestCsvStream) {
    BOOST_CHECK_THROW(lsst::qserv::mysql::CsvStream::create(0), std::invalid_argument);

    auto csvStream = lsst::qserv::mysql::CsvStream::create(2);

    std::thread producer([csvStream]() {
        csvStream->push("abc", 3);
        csvStream->push("def", 3);
        csvStream->push("0123456789", 10);
        csvStream->push(nullptr, 0);
    });

    std::thread consumer([csvStream]() {
        BOOST_CHECK_EQUAL(*csvStream->pop(), "abc");
        BOOST_CHECK_EQUAL(*csvStream->pop(), "def");
        BOOST_CHECK_EQUAL(*csvStream->pop(), "0123456789");
        BOOST_CHECK_EQUAL(*csvStream->pop(), std::string());
        BOOST_CHECK_EQUAL(csvStream->empty(), true);
    });
    producer.join();
    consumer.join();
    BOOST_CHECK_EQUAL(csvStream->empty(), true);
}

BOOST_AUTO_TEST_CASE(TestCsvStreamBuffer) {
    auto csvStream = lsst::qserv::mysql::CsvStream::create(2);
    auto csvBuf = lsst::qserv::mysql::newCsvStreamBuffer(csvStream);

    std::thread producer([csvStream]() {
        csvStream->push("abc", 3);
        csvStream->push("def", 3);
        csvStream->push("0123456789", 10);
        csvStream->push(nullptr, 0);
    });

    std::thread consumer([csvBuf]() {
        // Note: this test is based on thw assumption that the current implementation
        // of the class doesn't make an attempt to consolidate data from subsequent
        // input records into a single output buffer. This is important for the test to work
        // as expected. This is a reasonable assumption for the large records (where the ingest
        // performance is at stake), but it may be worth verifying this in the future.
        char buffer[20];
        BOOST_CHECK_EQUAL(csvBuf->fetch(buffer, 1), 1);
        BOOST_CHECK_EQUAL(std::string(buffer, 1), "a");
        BOOST_CHECK_EQUAL(csvBuf->fetch(buffer, 4), 2);
        BOOST_CHECK_EQUAL(std::string(buffer, 2), "bc");
        BOOST_CHECK_EQUAL(csvBuf->fetch(buffer, 10), 3);
        BOOST_CHECK_EQUAL(std::string(buffer, 3), "def");
        BOOST_CHECK_EQUAL(csvBuf->fetch(buffer, sizeof(buffer)), 10);
        BOOST_CHECK_EQUAL(std::string(buffer, 10), "0123456789");
        BOOST_CHECK_EQUAL(csvBuf->fetch(buffer, sizeof(buffer)), 0);
    });
    producer.join();
    consumer.join();
}

BOOST_AUTO_TEST_SUITE_END()
