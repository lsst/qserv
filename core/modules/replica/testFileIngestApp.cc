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
  * @brief test FileIngestApp
  */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/FileIngestApp.h"

// Boost unit test header
#define BOOST_TEST_MODULE FileIngestApp
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(FileIngestAppTest_parseFileList) {

    LOGS_INFO("FileIngestApp::parseFileList test begins");

    list<FileIngestApp::FileIngestSpec> fileSpecList;
    json obj;
    string str;

    str = "[]";
    BOOST_REQUIRE_NO_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    });
    BOOST_CHECK(fileSpecList.size() == 0);

    str = "{}";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    str =
        "["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]";
    BOOST_REQUIRE_NO_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    });
    BOOST_CHECK(fileSpecList.size() == 1);
    auto const& fileSpec = fileSpecList.front();
    BOOST_CHECK(fileSpec.workerHost == "worker-A");
    BOOST_CHECK(fileSpec.workerPort == 25002);
    BOOST_CHECK(fileSpec.transactionId == 1);
    BOOST_CHECK(fileSpec.tableName == "Object");
    BOOST_CHECK(fileSpec.tableType == "P");
    BOOST_CHECK(fileSpec.inFileName == "/tmp/chunk_123.txt");

    str =
        "["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"},"
        "{\"worker-host\":\"worker-B\","
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Filter\","
        "\"type\":\"R\","
        "\"path\":\"/tmp/Filter.txt\"}"
        "]";
    BOOST_REQUIRE_NO_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    });
    BOOST_CHECK(fileSpecList.size() == 2);

    // The array must contain objects
    str =
        "[["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]]";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    // Port number can't be less than 1
    str =
        "["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":0,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    // Table type could be R or P
    str =
        "["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"B\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    // Worker host needs to be a string, not a number
    str =
        "["
        "{\"worker-host\":9999,"
        "\"worker-port\":25002,"
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    // Worker port needs to be a number, not a string
    str =
        "["
        "{\"worker-host\":\"worker-A\","
        "\"worker-port\":\"25002\","
        "\"transaction-id\":1,"
        "\"table\":\"Object\","
        "\"type\":\"P\","
        "\"path\":\"/tmp/chunk_123.txt\"}"
        "]";
    BOOST_CHECK_THROW({
        obj = json::parse(str);
        fileSpecList = FileIngestApp::parseFileList(obj);
    }, invalid_argument);

    LOGS_INFO("FileIngestApp::parseFileList test ends");
}


BOOST_AUTO_TEST_CASE(FileIngestAppTest_parseChunkContribution) {

    LOGS_INFO("FileIngestApp::parseChunkContribution test begins");

    FileIngestApp::ChunkContribution contrib;
    BOOST_CHECK(contrib.chunk == 0);
    BOOST_CHECK(not contrib.isOverlap);

    BOOST_REQUIRE_NO_THROW({
        contrib = FileIngestApp::parseChunkContribution("chunk_1.txt");
    });
    BOOST_CHECK(contrib.chunk == 1);
    BOOST_CHECK(not contrib.isOverlap);

    BOOST_REQUIRE_NO_THROW({
        contrib = FileIngestApp::parseChunkContribution("chunk_2_overlap.txt");
    });
    BOOST_CHECK(contrib.chunk == 2);
    BOOST_CHECK(contrib.isOverlap);

    BOOST_CHECK_THROW({
        contrib = FileIngestApp::parseChunkContribution("path/chunk_2_overlap.txt");
    }, invalid_argument);

    BOOST_CHECK_THROW({
        contrib = FileIngestApp::parseChunkContribution("chunk_2_overlap");
    }, invalid_argument);

    BOOST_CHECK_THROW({
        contrib = FileIngestApp::parseChunkContribution("chunk_");
    }, invalid_argument);

    BOOST_CHECK_THROW({
        contrib = FileIngestApp::parseChunkContribution("test_2.txt");
    }, invalid_argument);

    LOGS_INFO("FileIngestApp::parseChunkContribution test ends");
}

BOOST_AUTO_TEST_SUITE_END()
