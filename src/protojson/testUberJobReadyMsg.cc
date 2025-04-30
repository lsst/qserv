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
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>

#include "nlohmann/json.hpp"

// Qserv headers
#include "global/clock_defs.h"
#include "http/MetaModule.h"
#include "lsst/log/Log.h"
#include "protojson/UberJobReadyMsg.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.testUberJobReadyMsg");
}

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::protojson;

string const repliInstanceId = "repliInstId";
string const repliAuthKey = "repliIAuthKey";
unsigned int const version = lsst::qserv::http::MetaModule::version;

BOOST_AUTO_TEST_SUITE(Suite)

bool parseSerializeReparseCheck(string const& jsStr, string const& note) {
    string fName("parseSerialize ");
    fName += note + " ";
    LOGS(_log, LOG_LVL_INFO, fName << " start " << jsStr);
    nlohmann::json js = nlohmann::json::parse(jsStr);
    LOGS(_log, LOG_LVL_INFO, fName << " parse 1");

    UberJobReadyMsg::Ptr jrm = UberJobReadyMsg::createFromJson(js);
    BOOST_REQUIRE(jrm != nullptr);

    nlohmann::json jsJrm = jrm->toJson();
    LOGS(_log, LOG_LVL_INFO, fName << " serialized jsJrm=" << jsJrm);

    UberJobReadyMsg::Ptr jrmCreated = UberJobReadyMsg::createFromJson(jsJrm);
    LOGS(_log, LOG_LVL_INFO, fName << " created");
    nlohmann::json jsJrmCreated = jrmCreated->toJson();
    LOGS(_log, LOG_LVL_INFO, fName << " created->serialized");

    bool createdMatchesOriginal = jsJrm == jsJrmCreated;
    if (createdMatchesOriginal) {
        LOGS(_log, LOG_LVL_INFO, fName << "created matches original");
    } else {
        LOGS(_log, LOG_LVL_ERROR, "jsJrm != jsJrmCreated");
        LOGS(_log, LOG_LVL_ERROR, "jsJrm=" << jsJrm);
        LOGS(_log, LOG_LVL_ERROR, "jsJrmCreated=" << jsJrmCreated);
    }
    BOOST_REQUIRE(createdMatchesOriginal);
    return createdMatchesOriginal;
}

BOOST_AUTO_TEST_CASE(WorkerQueryStatusData) {
    LOGS(_log, LOG_LVL_INFO, "testJRM start");

    string const workerIdStr("wrker72");
    string const czarName("cz4242");
    lsst::qserv::CzarIdType const czarId = 745;
    lsst::qserv::QueryId const queryId = 986532;
    lsst::qserv::UberJobId const uberJobId = 14578;
    string const fileUrl("ht.qwrk/some/dir/fil.txt");
    uint64_t const rowCount = 391;
    uint64_t const fileSize = 5623;

    auto jrm = UberJobReadyMsg::create(repliInstanceId, repliAuthKey, version, workerIdStr, czarName, czarId,
                                       queryId, uberJobId, fileUrl, rowCount, fileSize);

    auto jsJrm = jrm->toJson();
    string const strJrm = to_string(jsJrm);
    LOGS(_log, LOG_LVL_INFO, "stdJrm=" << strJrm);

    BOOST_REQUIRE(parseSerializeReparseCheck(strJrm, "A"));
}

BOOST_AUTO_TEST_SUITE_END()
