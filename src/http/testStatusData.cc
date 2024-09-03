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

// Qserv headers
#include "global/clock_defs.h"
#include "http/WorkerQueryStatusData.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::http;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(CzarContactInfo) {
    string const replicationInstanceId = "repliInstId";
    string const replicationAuthKey = "repliIAuthKey";

    string const czrName("czar_name");
    lsst::qserv::CzarIdType const czrId = 32;
    int czrPort = 2022;
    string const czrHost("cz_host");

    auto czarA = lsst::qserv::http::CzarContactInfo::create(czrName, czrId, czrPort, czrHost);
    LOGS_ERROR("&&& a czarA=" << czarA->dump());

    auto czarAJs = czarA->serializeJson();
    LOGS_ERROR("&&& b czarAJs=" << czarAJs);

    auto czarB = lsst::qserv::http::CzarContactInfo::createJson(czarAJs);
    LOGS_ERROR("&&& c czarB=" << czarB);
    BOOST_REQUIRE(czarA->compare(*czarB));

    auto czarC = lsst::qserv::http::CzarContactInfo::create("different", czrId, czrPort, czrHost);
    BOOST_REQUIRE(!czarA->compare(*czarC));

    auto start = lsst::qserv::CLOCK::now();
    auto workerA = WorkerContactInfo::create("sd_workerA", "host_w1", "mgmhost_a", 3421, start);
    auto workerB = WorkerContactInfo::create("sd_workerB", "host_w2", "mgmhost_a", 3421, start);
    auto workerC = WorkerContactInfo::create("sd_workerC", "host_w3", "mgmhost_b", 3422, start);
    LOGS_ERROR("&&& d workerA=" << workerA->dump());

    auto jsWorkerA = workerA->serializeJson();
    LOGS_ERROR("&&& e jsWorkerA=" << jsWorkerA);
    auto start1Sec = start + 1s;
    auto workerA1 = WorkerContactInfo::createJson(jsWorkerA, start1Sec);
    LOGS_ERROR("&&& f workerA1=" << workerA1->dump());
    BOOST_REQUIRE(workerA->isSameContactInfo(*workerA1));

    // WorkerQueryStatusData
    auto wqsdA = lsst::qserv::http::WorkerQueryStatusData::create(workerA, czarA, replicationInstanceId,
                                                                  replicationAuthKey);
    LOGS_ERROR("&&& g wqsdA=" << wqsdA->dump());

    //&&&double timeoutAliveSecs = 100.0;
    //&&&double timeoutDeadSecs = 2*timeoutAliveSecs;
    double maxLifetime = 300.0;
    auto jsDataA = wqsdA->serializeJson(maxLifetime);
    LOGS_ERROR("&&& h jsDataA=" << *jsDataA);

    // Check that empty lists work.
    auto wqsdA1 = lsst::qserv::http::WorkerQueryStatusData::createJson(*jsDataA, replicationInstanceId,
                                                                       replicationAuthKey, start1Sec);
    LOGS_ERROR("&&& i wqsdA1=" << wqsdA1->dump());
    auto jsDataA1 = wqsdA1->serializeJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA == *jsDataA1);

    vector<lsst::qserv::QueryId> qIdsDelFiles = {7, 8, 9, 15, 25, 26, 27, 30};
    vector<lsst::qserv::QueryId> qIdsKeepFiles = {1, 2, 3, 4, 6, 10, 13, 19, 33};
    for (auto const qIdDF : qIdsDelFiles) {
        wqsdA->_qIdDoneDeleteFiles[qIdDF] = start;
    }

    jsDataA = wqsdA->serializeJson(maxLifetime);
    LOGS_ERROR("&&& j jsDataA=" << jsDataA);
    BOOST_REQUIRE(*jsDataA != *jsDataA1);

    for (auto const qIdKF : qIdsKeepFiles) {
        wqsdA->_qIdDoneKeepFiles[qIdKF] = start;
    }

    wqsdA->addDeadUberJobs(12, {1, 3}, start);

    LOGS_ERROR("&&& i wqsdA=" << wqsdA->dump());

    jsDataA = wqsdA->serializeJson(maxLifetime);
    LOGS_ERROR("&&& j jsDataA=" << *jsDataA);

    auto start5Sec = start + 5s;
    auto workerAFromJson = lsst::qserv::http::WorkerQueryStatusData::createJson(
            *jsDataA, replicationInstanceId, replicationAuthKey, start5Sec);
    auto jsWorkerAFromJson = workerAFromJson->serializeJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA == *jsWorkerAFromJson);

    wqsdA->addDeadUberJobs(12, {34}, start5Sec);
    wqsdA->addDeadUberJobs(91, {77}, start5Sec);
    wqsdA->addDeadUberJobs(1059, {1, 4, 6, 7, 8, 10, 3, 22, 93}, start5Sec);

    jsDataA = wqsdA->serializeJson(maxLifetime);
    LOGS_ERROR("&&& k jsDataA=" << *jsDataA);
    BOOST_REQUIRE(*jsDataA != *jsWorkerAFromJson);

    workerAFromJson = lsst::qserv::http::WorkerQueryStatusData::createJson(*jsDataA, replicationInstanceId,
                                                                           replicationAuthKey, start5Sec);
    jsWorkerAFromJson = workerAFromJson->serializeJson(maxLifetime);
    LOGS_ERROR("&&& l jsWorkerAFromJson=" << *jsWorkerAFromJson);
    BOOST_REQUIRE(*jsDataA == *jsWorkerAFromJson);

    // Make the response, which contains lists of the items handled by the workers.
    auto jsWorkerResp = workerAFromJson->serializeResponseJson();

    // test removal of elements after response.
    BOOST_REQUIRE(!wqsdA->_qIdDoneDeleteFiles.empty());
    BOOST_REQUIRE(!wqsdA->_qIdDoneKeepFiles.empty());
    BOOST_REQUIRE(!wqsdA->_qIdDeadUberJobs.empty());

    wqsdA->handleResponseJson(jsWorkerResp);

    BOOST_REQUIRE(wqsdA->_qIdDoneDeleteFiles.empty());
    BOOST_REQUIRE(wqsdA->_qIdDoneKeepFiles.empty());
    BOOST_REQUIRE(wqsdA->_qIdDeadUberJobs.empty());
}

BOOST_AUTO_TEST_SUITE_END()
