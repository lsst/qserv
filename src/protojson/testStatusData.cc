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
#include "global/intTypes.h"
#include "lsst/log/Log.h"
#include "protojson/ResponseMsg.h"
#include "protojson/ScanTableInfo.h"
#include "protojson/UberJobErrorMsg.h"
#include "protojson/UberJobReadyMsg.h"
#include "protojson/WorkerCzarComIssue.h"
#include "protojson/WorkerQueryStatusData.h"
#include "util/Error.h"
#include "util/MultiError.h"
#include "wbase/UberJobData.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::protojson;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.testStatusData");
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(WorkerQueryStatusData) {
    lsst::qserv::protojson::AuthContext authContext_("repliInstId", "repliIAuthKey");

    uint64_t cxrStartTime = lsst::qserv::millisecSinceEpoch(lsst::qserv::CLOCK::now() - 5s);
    uint64_t wkrStartTime = lsst::qserv::millisecSinceEpoch(lsst::qserv::CLOCK::now() - 10s);

    string const czrName("czar_name");
    lsst::qserv::CzarId const czrId = 32;
    int czrPort = 2022;
    string const czrHost("cz_host");

    auto czarA =
            lsst::qserv::protojson::CzarContactInfo::create(czrName, czrId, czrPort, czrHost, cxrStartTime);

    auto czarAJs = czarA->toJson();

    auto czarB = lsst::qserv::protojson::CzarContactInfo::createFromJson(czarAJs);
    BOOST_REQUIRE(czarA->compare(*czarB));

    auto czarC = lsst::qserv::protojson::CzarContactInfo::create("different", czrId, czrPort, czrHost,
                                                                 cxrStartTime);
    BOOST_REQUIRE(!czarA->compare(*czarC));

    auto start = lsst::qserv::CLOCK::now();
    auto workerA = WorkerContactInfo::create("sd_workerA", "host_w1", "mgmhost_a", 3421, start);

    auto workerB = WorkerContactInfo::create("sd_workerB", "host_w2", "mgmhost_a", 3421, start);
    auto workerC = WorkerContactInfo::create("sd_workerC", "host_w3", "mgmhost_b", 3422, start);

    auto jsWorkerA = workerA->toJson();
    auto start1Sec = start + 1s;
    auto workerA1 = WorkerContactInfo::createFromJsonWorker(jsWorkerA, start1Sec);
    BOOST_REQUIRE(workerA->isSameContactInfo(*workerA1));

    // WorkerQueryStatusData
    auto wqsdA = lsst::qserv::protojson::WorkerQueryStatusData::create(workerA, czarA, authContext_);

    double maxLifetime = 300.0;
    auto jsDataA = wqsdA->toJson(maxLifetime);

    // Check that empty lists work.
    auto wqsdA1 =
            lsst::qserv::protojson::WorkerQueryStatusData::createFromJson(*jsDataA, authContext_, start1Sec);

    auto jsDataA1 = wqsdA1->toJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA == *jsDataA1);

    vector<lsst::qserv::QueryId> qIdsDelFiles = {7, 8, 9, 15, 25, 26, 27, 30};
    vector<lsst::qserv::QueryId> qIdsKeepFiles = {1, 2, 3, 4, 6, 10, 13, 19, 33};
    for (auto const qIdDF : qIdsDelFiles) {
        wqsdA->qIdDoneDeleteFiles[qIdDF] = start;
    }

    jsDataA = wqsdA->toJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA != *jsDataA1);

    for (auto const qIdKF : qIdsKeepFiles) {
        wqsdA->qIdDoneKeepFiles[qIdKF] = start;
    }

    wqsdA->addDeadUberJobs(12, {1, 3}, start);

    jsDataA = wqsdA->toJson(maxLifetime);

    auto start5Sec = start + 5s;
    auto workerAFromJson =
            lsst::qserv::protojson::WorkerQueryStatusData::createFromJson(*jsDataA, authContext_, start5Sec);
    auto jsWorkerAFromJson = workerAFromJson->toJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA == *jsWorkerAFromJson);

    wqsdA->addDeadUberJobs(12, {34}, start5Sec);
    wqsdA->addDeadUberJobs(91, {77}, start5Sec);
    wqsdA->addDeadUberJobs(1059, {1, 4, 6, 7, 8, 10, 3, 22, 93}, start5Sec);

    jsDataA = wqsdA->toJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA != *jsWorkerAFromJson);

    workerAFromJson =
            lsst::qserv::protojson::WorkerQueryStatusData::createFromJson(*jsDataA, authContext_, start5Sec);
    jsWorkerAFromJson = workerAFromJson->toJson(maxLifetime);
    BOOST_REQUIRE(*jsDataA == *jsWorkerAFromJson);

    // Make the response, which contains lists of the items handled by the workers.
    auto jsWorkerResp = workerAFromJson->buildResponseJson(wkrStartTime);

    // test removal of elements after response.
    BOOST_REQUIRE(!wqsdA->qIdDoneDeleteFiles.empty());
    BOOST_REQUIRE(!wqsdA->qIdDoneKeepFiles.empty());
    BOOST_REQUIRE(!wqsdA->qIdDeadUberJobs.empty());

    wqsdA->handleResponseJson(jsWorkerResp);
    auto workerRestarted = wqsdA->handleResponseJson(jsWorkerResp);
    BOOST_REQUIRE(workerRestarted == false);

    BOOST_REQUIRE(wqsdA->qIdDoneDeleteFiles.empty());
    BOOST_REQUIRE(wqsdA->qIdDoneKeepFiles.empty());
    BOOST_REQUIRE(wqsdA->qIdDeadUberJobs.empty());
}

BOOST_AUTO_TEST_CASE(WorkerCzarComIssue) {
    AuthContext authContext_("repliInstId", "repliIAuthKey");

    uint64_t cxrStartTime = lsst::qserv::millisecSinceEpoch(lsst::qserv::CLOCK::now() - 5s);

    string const czrName("czar_name");
    lsst::qserv::CzarId const czrId = 32;
    int czrPort = 2022;
    string const czrHost("cz_host");

    auto czarA =
            lsst::qserv::protojson::CzarContactInfo::create(czrName, czrId, czrPort, czrHost, cxrStartTime);
    auto czarAJs = czarA->toJson();

    auto start = lsst::qserv::CLOCK::now();
    auto workerA = WorkerContactInfo::create("sd_workerA", "host_w1", "mgmhost_a", 3421, start);
    auto jsWorkerA = workerA->toJson();

    // WorkerCzarComIssue, thought czar was dead test.
    auto wccIssueA = lsst::qserv::protojson::WorkerCzarComIssue::create(authContext_);
    wccIssueA->setContactInfo(workerA, czarA);
    BOOST_REQUIRE(wccIssueA->needToSend() == false);
    wccIssueA->setThoughtCzarWasDeadTime(3452987);
    BOOST_REQUIRE(wccIssueA->needToSend() == true);

    auto jsIssueA = wccIssueA->toJson();

    // The source WorkerCzarComIssue for failed transmit tests.
    auto wccIssueA1 = lsst::qserv::protojson::WorkerCzarComIssue::createFromJson(jsIssueA, authContext_);
    auto jsIssueA1 = wccIssueA1->toJson();
    BOOST_REQUIRE(jsIssueA == jsIssueA1);

    // A vector of the expected responses from the czar from UberJobData responses
    // added to wccIssueA1 as failed transmits.
    std::vector<ExecutiveRespMsg::Ptr> czarResponseMsgs;

    // Test a list of failed messages.
    string const czarHost = "czarHost";
    int const czarPort = 234;
    string const czarName = "czar1";
    lsst::qserv::CzarId const czarId = 1;
    string const workerId1 = "wrkr1";
    int const resultPort = 436;
    int const rowlimit = 0;
    int const maxTableBytes = 1'000'000;

    lsst::qserv::UberJobId const ujId1 = 1;
    lsst::qserv::QueryId const qId1 = 722234;
    bool const scaninteractive1 = true;
    auto scanInfo1 = lsst::qserv::protojson::ScanInfo::create();
    uint64_t const rowCount1 = 81;
    uint64_t const fileSize1 = 1240;
    FileUrlInfo fileInf1("http://test/ulr1/fn", rowCount1, fileSize1);
    auto ujData1 = lsst::qserv::wbase::UberJobData::create(
            ujId1, czarName, czarId, czarHost, czarPort, qId1, rowlimit, maxTableBytes, scanInfo1,
            scaninteractive1, workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujResponse1 = ujData1->responseFileReadyBuild(fileInf1, authContext_);
    wccIssueA1->addFailedTransmit(qId1, ujId1, ujResponse1);
    auto execRespMsg1 = ExecutiveRespMsg::create(true, false, qId1, ujId1, czarId);
    czarResponseMsgs.push_back(execRespMsg1);

    auto jsWcA1 = wccIssueA1->toJson();
    // parse jsWcA1 and check if the answer is correct
    auto wccIssueA1Out1 = lsst::qserv::protojson::WorkerCzarComIssue::createFromJson(jsWcA1, authContext_);
    BOOST_REQUIRE(*wccIssueA1 == *wccIssueA1Out1);

    lsst::qserv::QueryId const qId1a = qId1;
    lsst::qserv::UberJobId const ujId1a = 9;
    lsst::qserv::protojson::FileUrlInfo fileInf1a("http://test/ulr1/fna", 36, 12400);
    auto ujData1a = lsst::qserv::wbase::UberJobData::create(
            ujId1a, czarName, czarId, czarHost, czarPort, qId1, rowlimit, maxTableBytes, scanInfo1,
            scaninteractive1, workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujResponse1a = ujData1->responseFileReadyBuild(fileInf1a, authContext_);
    wccIssueA1->addFailedTransmit(qId1a, ujId1a, ujResponse1a);
    auto execRespMsg1a = ExecutiveRespMsg::create(true, true, qId1a, ujId1a, czarId);
    czarResponseMsgs.push_back(execRespMsg1a);

    auto jsWcA1a = wccIssueA1->toJson();
    // parse jsWcA1a and check if the answer is correct
    auto wccIssueA1aOut1 = lsst::qserv::protojson::WorkerCzarComIssue::createFromJson(jsWcA1a, authContext_);
    BOOST_REQUIRE(*wccIssueA1 == *wccIssueA1aOut1);
    BOOST_REQUIRE(*wccIssueA1 != *wccIssueA1Out1);

    lsst::qserv::UberJobId const ujId2 = 333;
    lsst::qserv::QueryId qId2 = 722237;
    bool scaninteractive2 = false;
    lsst::qserv::protojson::FileUrlInfo fileInf2("http://test/ulr2/fn", 456, 424000);
    auto scanInfo2 = lsst::qserv::protojson::ScanInfo::create();
    auto ujData2 = lsst::qserv::wbase::UberJobData::create(
            ujId2, czarName, czarId, czarHost, czarPort, qId2, rowlimit, maxTableBytes, scanInfo2,
            scaninteractive2, workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujResponse2 = ujData2->responseFileReadyBuild(fileInf2, authContext_);
    wccIssueA1->addFailedTransmit(qId2, ujId2, ujResponse2);
    auto execRespMsg2 = ExecutiveRespMsg::create(true, false, qId2, ujId2, czarId);
    czarResponseMsgs.push_back(execRespMsg2);

    auto jsWcA2 = wccIssueA1->toJson();
    // parse jsWcA2 and check if the answer is correct
    auto wccIssueA2Out1 = lsst::qserv::protojson::WorkerCzarComIssue::createFromJson(jsWcA2, authContext_);
    BOOST_REQUIRE(*wccIssueA1 == *wccIssueA2Out1);

    lsst::qserv::UberJobId const ujId3 = 8;
    lsst::qserv::QueryId qId3 = 722240;
    int const chunkId3 = 471;
    bool const cancelled3 = true;
    lsst::qserv::util::MultiError multiErr;
    lsst::qserv::util::Error err(105423, 3, "Some random error.");
    multiErr.insert(err);
    auto ujData3 = lsst::qserv::wbase::UberJobData::create(
            ujId3, czarName, czarId, czarHost, czarPort, qId3, rowlimit, maxTableBytes, scanInfo2,
            scaninteractive2, workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujResponse3 =
            ujData3->responseErrorBuild(multiErr, chunkId3, cancelled3, LOG_LVL_DEBUG, authContext_);
    wccIssueA1->addFailedTransmit(qId3, ujId3, ujResponse3);
    auto execRespMsg3 = ExecutiveRespMsg::create(true, true, qId3, ujId3, czarId);
    czarResponseMsgs.push_back(execRespMsg3);

    auto jsWcA3 = wccIssueA1->toJson();
    // parse jsWcA3 and check if the answer is correct
    auto wccIssueA3Out1 = lsst::qserv::protojson::WorkerCzarComIssue::createFromJson(jsWcA3, authContext_);
    LOGS(_log, LOG_LVL_INFO, "wccIssueA1=" << wccIssueA1->dump());
    LOGS(_log, LOG_LVL_INFO, "wccIssueA3Out1=" << wccIssueA3Out1->dump());
    BOOST_REQUIRE(*wccIssueA1 != *wccIssueA2Out1);
    BOOST_REQUIRE(*wccIssueA1 == *wccIssueA3Out1);

    // Add the ExecutiveRespMsg messages that correlate to the failed transmits to
    // the czar response message so they can be used to clear the map entries.
    auto czarRespMsgJson =
            wccIssueA3Out1->responseToJson(wccIssueA3Out1->getThoughtCzarWasDeadTime(), czarResponseMsgs);
    LOGS(_log, LOG_LVL_INFO, "czarRespMsgJson=" << czarRespMsgJson);

    // Parse the response and remove the appropriate entries from wccIsueA1.
    auto czRespMsg = lsst::qserv::protojson::WorkerCzarComRespMsg::createFromJson(czarRespMsgJson);
    LOGS(_log, LOG_LVL_INFO, "czRespMsg=" << czRespMsg->dump());
    BOOST_REQUIRE(czRespMsg->success == true);

    // Use the response to clear the original transmit failure messages from the originating worker.
    auto [countA3Out1, obsoleteList, parseErrorList] = wccIssueA1->clearMapEntries(czarRespMsgJson);
    LOGS(_log, LOG_LVL_INFO,
         "countA3Out1=" << countA3Out1 << " obsoleteSz=" << obsoleteList.size()
                        << " parseErrorSz=" << parseErrorList.size());
    BOOST_REQUIRE(countA3Out1 == 4);
    BOOST_REQUIRE(obsoleteList.size() == 2);
    BOOST_REQUIRE(parseErrorList.size() == 0);

    auto ftMap = wccIssueA1->takeFailedTransmitsMap();
    BOOST_REQUIRE(ftMap->size() == 0);
}

BOOST_AUTO_TEST_CASE(WorkerCzarComIssueClearFailedTransmitsForQids) {
    AuthContext authContext_("repliInstId", "repliIAuthKey");

    uint64_t czStartTime = lsst::qserv::millisecSinceEpoch(lsst::qserv::CLOCK::now() - 5s);
    string const czName("czar_name");
    lsst::qserv::CzarId const czId = 32;
    int czPort = 2022;
    string const czHost("cz_host");

    auto czarA = lsst::qserv::protojson::CzarContactInfo::create(czName, czId, czPort, czHost, czStartTime);

    auto start = lsst::qserv::CLOCK::now();
    auto workerA = WorkerContactInfo::create("sd_workerA", "host_w1", "mgmhost_a", 3421, start);

    // Create the WorkerCzarComIssue and set contact info
    auto wcc = lsst::qserv::protojson::WorkerCzarComIssue::create(authContext_);
    wcc->setContactInfo(workerA, czarA);

    // Build three failed transmit messages:
    // - qIdA has two UberJobs (uj 1 and uj 2)
    // - qIdB has one UberJob (uj 3)
    lsst::qserv::QueryId const qIdA = 1001;
    lsst::qserv::QueryId const qIdB = 2002;
    lsst::qserv::UberJobId const ujA1 = 1;
    lsst::qserv::UberJobId const ujA2 = 2;
    lsst::qserv::UberJobId const ujB1 = 3;

    string const workerId1 = "wrkr1";
    int const resultPort = 436;
    int const rowlimit = 0;
    int const maxTableBytes = 1'000'000;
    bool const scaninteractive = true;
    auto scanInfo = lsst::qserv::protojson::ScanInfo::create();

    // create UberJobData for qIdA ujA1
    lsst::qserv::protojson::FileUrlInfo fileInf1("http://test/fn1", 10, 100);
    auto ujDataA1 = lsst::qserv::wbase::UberJobData::create(
            ujA1, czName, czId, czHost, czPort, qIdA, rowlimit, maxTableBytes, scanInfo, scaninteractive,
            workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujRespA1 = ujDataA1->responseFileReadyBuild(fileInf1, authContext_);
    wcc->addFailedTransmit(qIdA, ujA1, ujRespA1);

    // create UberJobData for qIdA ujA2
    lsst::qserv::protojson::FileUrlInfo fileInf2("http://test/fn2", 20, 200);
    auto ujDataA2 = lsst::qserv::wbase::UberJobData::create(
            ujA2, czName, czId, czHost, czPort, qIdA, rowlimit, maxTableBytes, scanInfo, scaninteractive,
            workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujRespA2 = ujDataA2->responseFileReadyBuild(fileInf2, authContext_);
    wcc->addFailedTransmit(qIdA, ujA2, ujRespA2);

    // create UberJobData for qIdB ujB1
    lsst::qserv::protojson::FileUrlInfo fileInf3("http://test/fn3", 30, 300);
    auto ujDataB1 = lsst::qserv::wbase::UberJobData::create(
            ujB1, czName, czId, czHost, czPort, qIdB, rowlimit, maxTableBytes, scanInfo, scaninteractive,
            workerId1, nullptr, authContext_.replicationAuthKey, resultPort);
    auto ujRespB1 = ujDataB1->responseFileReadyBuild(fileInf3, authContext_);
    wcc->addFailedTransmit(qIdB, ujB1, ujRespB1);

    // Confirm three failed transmits exist via JSON representation
    auto jsBefore = wcc->toJson();
    BOOST_REQUIRE(jsBefore.contains("failedtransmits"));
    auto const& arrBefore = jsBefore["failedtransmits"];
    BOOST_REQUIRE(arrBefore.is_array());
    BOOST_REQUIRE(arrBefore.size() == 3);

    // Prepare a map with qIdA to be cleared
    std::map<lsst::qserv::QueryId, lsst::qserv::TIMEPOINT> qIdMap;
    qIdMap[qIdA] = lsst::qserv::CLOCK::now();

    // Call clearFailedTransmitsForQids
    wcc->clearFailedTransmitsForQids(qIdMap);

    // Verify only the qIdB entry remains
    auto jsAfter = wcc->toJson();
    BOOST_REQUIRE(jsAfter.contains("failedtransmits"));
    auto const& arrAfter = jsAfter["failedtransmits"];
    BOOST_REQUIRE(arrAfter.is_array());
    BOOST_REQUIRE(arrAfter.size() == 1);
    // remaining element should have qId == qIdB
    auto rem = arrAfter[0];
    BOOST_REQUIRE(rem.contains("qId"));
    lsst::qserv::QueryId remQ = rem["qId"];
    BOOST_REQUIRE(remQ == qIdB);
}

BOOST_AUTO_TEST_CASE(ResponseMsg) {
    auto respMsgA = lsst::qserv::protojson::ResponseMsg::create(true);
    auto jsA = respMsgA->toJson();
    auto respMsgAOut = lsst::qserv::protojson::ResponseMsg::createFromJson(jsA);
    BOOST_REQUIRE(respMsgA->equal(*respMsgAOut));

    auto respMsgB = lsst::qserv::protojson::ResponseMsg::create(false, "asdrewjgfay523yuq@", "junk msg");
    auto respMsgC = lsst::qserv::protojson::ResponseMsg::create(false, "asd", "junk msg");
    auto respMsgD = lsst::qserv::protojson::ResponseMsg::create(false, "asdrewjgfay523yuq@", "junkmsg");
    auto jsB = respMsgB->toJson();
    auto respMsgBOut = lsst::qserv::protojson::ResponseMsg::createFromJson(jsB);
    BOOST_REQUIRE(respMsgB->equal(*respMsgBOut));
    BOOST_REQUIRE(!respMsgA->equal(*respMsgBOut));
    BOOST_REQUIRE(!respMsgB->equal(*respMsgC));
    BOOST_REQUIRE(!respMsgD->equal(*respMsgC));
}

BOOST_AUTO_TEST_CASE(ExecutiveRespMsg) {
    auto respMsgA = lsst::qserv::protojson::ExecutiveRespMsg::create(true, false, 123, 456, 9, "allGood",
                                                                     "just a test");
    auto jsA = respMsgA->toJson();
    auto respMsgAOut = lsst::qserv::protojson::ExecutiveRespMsg::createFromJson(jsA);
    BOOST_REQUIRE(respMsgA->equal(*respMsgAOut));
}

BOOST_AUTO_TEST_CASE(WorkerCzarComRespMsg) {
    auto respMsgA = lsst::qserv::protojson::WorkerCzarComRespMsg::create(true, 73);
    auto jsA = respMsgA->toJson();
    auto respMsgAOut = lsst::qserv::protojson::WorkerCzarComRespMsg::createFromJson(jsA);
    BOOST_REQUIRE(respMsgA->equal(*respMsgAOut));
}

BOOST_AUTO_TEST_SUITE_END()
