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
#include "protojson/UberJobErrorMsg.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.testUberJobErrorMsg");
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

    UberJobErrorMsg::Ptr jrm = UberJobErrorMsg::createFromJson(js, repliInstanceId, repliAuthKey);
    BOOST_REQUIRE(jrm != nullptr);

    auto jsJrm = jrm->toJsonPtr();
    LOGS(_log, LOG_LVL_INFO, fName << " serialized jsJrm=" << *jsJrm);

    UberJobErrorMsg::Ptr jrmCreated = UberJobErrorMsg::createFromJson(*jsJrm, repliInstanceId, repliAuthKey);
    LOGS(_log, LOG_LVL_INFO, fName << " created");
    auto jsJrmCreated = jrmCreated->toJsonPtr();
    LOGS(_log, LOG_LVL_INFO, fName << " created->serialized");

    bool createdMatchesOriginal = *jsJrm == *jsJrmCreated;
    if (createdMatchesOriginal) {
        LOGS(_log, LOG_LVL_INFO, fName << "created matches original");
    } else {
        LOGS(_log, LOG_LVL_ERROR, "jsJrm != jsJrmCreated");
        LOGS(_log, LOG_LVL_ERROR, "jsJrm=" << *jsJrm);
        LOGS(_log, LOG_LVL_ERROR, "jsJrmCreated=" << *jsJrmCreated);
    }
    BOOST_REQUIRE(createdMatchesOriginal);
    return createdMatchesOriginal;
}

BOOST_AUTO_TEST_CASE(WorkerQueryStatusData) {
    LOGS(_log, LOG_LVL_INFO, "testJRM start");

    string const workerIdStr("wrker72");
    string const czarName("cz4242");
    lsst::qserv::CzarId const czarId = 745;
    lsst::qserv::QueryId const queryId = 986532;
    lsst::qserv::UberJobId const uberJobId = 14578;
    string const errorMsg("something went wrong");
    int const errorCode = -3;

    auto jrm = UberJobErrorMsg::create(repliInstanceId, repliAuthKey, version, workerIdStr, czarName, czarId,
                                       queryId, uberJobId, errorCode, errorMsg);

    auto jsJrm = jrm->toJsonPtr();
    string const strJrm = to_string(*jsJrm);
    LOGS(_log, LOG_LVL_INFO, "stdJrm=" << strJrm);

    BOOST_REQUIRE(parseSerializeReparseCheck(strJrm, "A"));
}

BOOST_AUTO_TEST_SUITE_END()
