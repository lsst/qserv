// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// System headers
#include <sstream>
#include <string>
#include <vector>

#include "lsst/log/Log.h"

// Qserv headers
#include "http/MetaModule.h"
#include "protojson/QueryExplainMsg.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryExplainMsg
#include <boost/test/unit_test.hpp>

using namespace lsst::qserv;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.testQueryExplainMsg");
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(JsonRoundTripAndDump) {
    nlohmann::json const input = {
            {"instance_id", "instance"},
            {"auth_key", "secret"},
            {"version", http::MetaModule::version},
            {"worker", "worker-1"},
            {"czarid", 17},
            {"queryid", 29},
            {"dbname", "catalog"},
            {"chunkid", 41},
            {"subchunkid", 7},
            {"queries", {"SELECT 1", "SELECT 2"}},
            {"subchunktables", {{{"db", "catalog"}, {"table", "Object"}}}},
    };

    auto message = protojson::QueryExplainMsg::createFromJson(input);
    BOOST_REQUIRE(message != nullptr);
    LOGS(_log, LOG_LVL_DEBUG, "message=" << message->dump());
    BOOST_CHECK(message->queries == std::vector<std::string>({"SELECT 1", "SELECT 2"}));
    BOOST_CHECK(message->dbName == "catalog");
    BOOST_CHECK(message->chunkId == 41);
    BOOST_CHECK(message->subchunkId == 7);
    BOOST_REQUIRE_EQUAL(message->subchunkTables.size(), 1U);
    BOOST_CHECK(message->subchunkTables.begin()->db == "catalog");
    BOOST_CHECK(message->subchunkTables.begin()->table == "Object");
    BOOST_CHECK(message->toJson() == input);

    std::ostringstream dump;
    message->dump(dump);
    BOOST_CHECK_EQUAL(dump.str(),
                      "{QueryExplainMsg: queryId=29 workerId=worker-1 czarId=17 queryId=29 dbName=catalog "
                      "chunkId=41 subchunkId=7 queries=2{SELECT 1,SELECT 2,} "
                      "subchunkTables={{catalog,Object},}");
}

BOOST_AUTO_TEST_CASE(RejectsInvalidJson) {
    nlohmann::json message = {
            {"instance_id", "instance"}, {"auth_key", "secret"}, {"version", http::MetaModule::version + 1},
            {"worker", "worker-1"},      {"czarid", 17},         {"queryid", 29},
            {"dbname", "catalog"},       {"chunkid", 41},        {"subchunkid", 7},
            {"queries", {"SELECT 1"}},
    };
    BOOST_CHECK(protojson::QueryExplainMsg::createFromJson(message) == nullptr);

    message["version"] = http::MetaModule::version;
    message.erase("chunkid");
    BOOST_CHECK(protojson::QueryExplainMsg::createFromJson(message) == nullptr);

    message["chunkid"] = 41;
    message.erase("subchunkid");
    BOOST_CHECK(protojson::QueryExplainMsg::createFromJson(message) == nullptr);

    message["subchunkid"] = 7;
    message.erase("dbname");
    BOOST_CHECK(protojson::QueryExplainMsg::createFromJson(message) == nullptr);

    message["dbname"] = "catalog";
    message.erase("worker");
    BOOST_CHECK(protojson::QueryExplainMsg::createFromJson(message) == nullptr);
}

BOOST_AUTO_TEST_CASE(ResultElementJsonRoundTripAndDump) {
    auto message = protojson::QueryExplainResultElementMsg::create();
    message->explainResult = {{"query_block", {{"select_id", 1}}}};

    util::Error error(5001, 1064, "failed to explain", false);
    error.incrCount(2);
    message->multiError.insert(error);

    auto const json = message->toJson();
    auto decoded = protojson::QueryExplainResultElementMsg::createFromJson(json);
    BOOST_REQUIRE(decoded != nullptr);
    BOOST_CHECK(decoded->explainResult == message->explainResult);
    BOOST_CHECK(decoded->multiError == message->multiError);
    BOOST_CHECK_EQUAL(json["multiError"][0]["count"].get<int>(), 3);

    std::ostringstream dump;
    decoded->dump(dump);
    BOOST_CHECK_EQUAL(decoded->dump(), dump.str());
    BOOST_CHECK(dump.str().find("QueryExplainResultElementMsg") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(ResultMsgJsonRoundTripAndDump) {
    auto message = protojson::QueryExplainResultMsg::create();
    message->success = true;
    message->errorType = "none";
    message->note = "complete";
    message->queryId = 29;
    message->dbName = "catalog";
    message->chunkId = 41;
    message->subchunkId = 7;

    auto resultElement = protojson::QueryExplainResultElementMsg::create();
    resultElement->explainResult = {{"query_block", {{"select_id", 1}}}};
    message->resultElements.emplace_back(resultElement);

    auto const json = message->toJson();
    auto decoded = protojson::QueryExplainResultMsg::createFromJson(json);
    BOOST_REQUIRE(decoded != nullptr);
    BOOST_CHECK(decoded->success);
    BOOST_CHECK(decoded->errorType == "none");
    BOOST_CHECK(decoded->note == "complete");
    BOOST_CHECK_EQUAL(decoded->queryId, 29);
    BOOST_CHECK(decoded->dbName == "catalog");
    BOOST_CHECK_EQUAL(decoded->chunkId, 41);
    BOOST_CHECK_EQUAL(decoded->subchunkId, 7);
    BOOST_REQUIRE_EQUAL(decoded->resultElements.size(), 1U);
    BOOST_CHECK(decoded->resultElements.front()->explainResult == resultElement->explainResult);
    BOOST_CHECK(decoded->toJson() == json);

    std::ostringstream dump;
    decoded->dumpOs(dump);
    BOOST_CHECK_EQUAL(decoded->dump(), dump.str());
    BOOST_CHECK(dump.str().find("QueryExplainResultMsg") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(ResultMsgRejectsInvalidJson) {
    nlohmann::json message = {
            {"success", 1},
            {"errortype", "none"},
            {"note", "complete"},
            {"queryid", 29},
            {"chunkid", 41},
            {"subchunkid", 7},
            {"resultElements", nlohmann::json::array()},
    };
    BOOST_CHECK(protojson::QueryExplainResultMsg::createFromJson(message) != nullptr);

    message["resultElements"] = "not-an-array";
    BOOST_CHECK(protojson::QueryExplainResultMsg::createFromJson(message) == nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
