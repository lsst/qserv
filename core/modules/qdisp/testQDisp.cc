// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
#include <unistd.h>

// Third-party headers
#include "boost/lexical_cast.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE Qdisp_1
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MergingHandler.h"
#include "global/ResourceUnit.h"
#include "global/MsgReceiver.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "qdisp/LargeResultMgr.h"
#include "qdisp/MessageStore.h"
#include "qdisp/XrdSsiMocks.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/TaskMsgFactory.h"
#include "util/threadSafe.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.testQDisp");

typedef util::Sequential<int> SequentialInt;
typedef std::vector<qdisp::ResponseHandler::Ptr> RequesterVector;

class ChunkMsgReceiverMock : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
        LOGS_DEBUG("Mock::operator() chunkId=" << _chunkId
                   << ", code=" << code << ", msg=" << msg);
    }
    static std::shared_ptr<ChunkMsgReceiverMock> newInstance(int chunkId) {
        std::shared_ptr<ChunkMsgReceiverMock> r = std::make_shared<ChunkMsgReceiverMock>();
        r->_chunkId = chunkId;
        return r;
    }
    int _chunkId;
};

/** Simple functor for testing that _retryfunc has been called.
 */
class JobQueryTest : public qdisp::JobQuery {
public:
    typedef std::shared_ptr<JobQueryTest> Ptr;
    JobQueryTest(qdisp::Executive::Ptr const& executive,
                 qdisp::JobDescription::Ptr const& jobDescription,
                 qdisp::JobStatus::Ptr jobStatus,
                 qdisp::MarkCompleteFunc::Ptr markCompleteFunc)
        : qdisp::JobQuery{executive, jobDescription, jobStatus, markCompleteFunc, 12345} {}

    virtual ~JobQueryTest() {}
    virtual bool runJob() override {
        retryCalled = true;
        LOGS_DEBUG("_retryCalled=" << retryCalled);
        return true;
    }
    bool retryCalled {false};

    // Create a fresh JobQueryTest instance. If you're making this to get a QueryRequestObject,
    // set createQueryRequest=true and pass an xsSession pointer.
    // set createQueryResource=true to get a QueryResource.
    // Special ResponseHandlers need to be defined in JobDescription.
    static JobQueryTest::Ptr getJobQueryTest(
            qdisp::Executive::Ptr const& executive, qdisp::JobDescription::Ptr const& jobDesc,
            qdisp::MarkCompleteFunc::Ptr markCompleteFunc,
            bool createQueryResource, XrdSsiSession* xsSession, bool createQueryRequest) {
        qdisp::JobStatus::Ptr status(new qdisp::JobStatus());
        std::shared_ptr<JobQueryTest> jqTest(new JobQueryTest(executive, jobDesc, status, markCompleteFunc));
        jqTest->_setup(); // Must call _setup() by hand as bypassing newJobQuery().
        if (createQueryResource) {
            jqTest->_queryResourcePtr.reset(new qdisp::QueryResource(jqTest));
        }
        if (createQueryRequest) {
            jqTest->_queryRequestPtr.reset(new qdisp::QueryRequest(xsSession, jqTest));
        }
        return jqTest;
    }
};

/** Simple functor for testing _finishfunc.
 */
class FinishTest : public qdisp::MarkCompleteFunc {
public:
    typedef std::shared_ptr<FinishTest> Ptr;
    FinishTest() : MarkCompleteFunc(0, -1) {}
    virtual ~FinishTest() {}
    virtual void operator()(bool val) {
        finishCalled = true;
        LOGS_DEBUG("_finishCalled=" << finishCalled);
    }
    bool finishCalled {false};
};

/** Simple ResponseHandler for testing.
 */
class ResponseHandlerTest : public qdisp::ResponseHandler {
public:
    ResponseHandlerTest() : _code(0), _finished(false), _processCancelCalled(false) {}
    std::vector<char>& nextBuffer() override {
        return _vect;
    }
    size_t nextBufferSize() override {
        return _vect.size();
    }
    bool flush(int bLen, bool& last, bool& largeResult) override {
        return bLen == magic();
    }
    void errorFlush(std::string const& msg, int code) override {
        _msg = msg;
        _code = code;
    }
    bool finished() const override {
        return _finished;
    }
    bool reset() override {
        return true;
    }
    qdisp::ResponseHandler::Error getError() const override {
        return qdisp::ResponseHandler::Error(-1, "testQDisp Error");
    }
    std::ostream& print(std::ostream& os) const override {
        return os;
    }
    void processCancel() override {
        _processCancelCalled = true;
    }

    bool prepScrubResults(int jobId, int attempt) override {
        return true;
    }

    static int magic() {return 8;}
    std::vector<char> _vect;
    std::string _msg;
    int _code;
    bool _finished;
    bool _processCancelCalled;
};


namespace lsst {
namespace qserv {
namespace qproc {


// Normally, there's one TaskMsgFactory that all jobs in a user query share.
// In this case, there's one MockTaskMsgFactory per job with a payload specific for that job.
class MockTaskMsgFactory : public TaskMsgFactory {
public:
    MockTaskMsgFactory(std::string const& mockPayload_)
        : TaskMsgFactory(0), mockPayload(mockPayload_) {}
    void serializeMsg(ChunkQuerySpec const& s,
                      std::string const& chunkResultName,
                      uint64_t queryId, int jobId, int attemptCount,
                      std::ostream& os) override {
        os << mockPayload;
    }
    std::string mockPayload;
};

}}} // namespace lsst::qserv::qproc


qdisp::JobDescription::Ptr makeMockJobDescription(qdisp::Executive::Ptr const& ex, int sequence,
                                                  ResourceUnit const& ru, std::string msg,
                                                  std::shared_ptr<qdisp::ResponseHandler> const& mHandler) {
    auto mockTaskMsgFactory = std::make_shared<qproc::MockTaskMsgFactory>(msg);
    auto cqs = std::make_shared<qproc::ChunkQuerySpec>(); // dummy, unused in this case.
    std::string chunkResultName = "dummyResultTableName";
    auto job = qdisp::JobDescription::create(ex->getId(), sequence, ru, mHandler,
                                                        mockTaskMsgFactory, cqs, chunkResultName, true);
    return job;
}


/** Add dummy requests to an executive corresponding to the requesters
 */
void addMockRequests(qdisp::Executive::Ptr const& ex, SequentialInt &sequence, std::string millisecs, RequesterVector& rv) {
    ResourceUnit ru;
    int copies = rv.size();
    std::vector<std::shared_ptr<qdisp::JobDescription>> s(copies);
    for(int j=0; j < copies; ++j) {
        // The job copies the JobDescription.
        qdisp::JobDescription::Ptr job = makeMockJobDescription(ex, sequence.incr(), ru,
                                                millisecs, rv[j]);
        auto jobQuery = ex->add(job); // ex->add() is not thread safe.
    }
}

/** Start adds 'copies' number of test requests that each sleep for 'millisecs' time
 * before signaling to 'ex' that they are done.
 * Returns time to complete in seconds.
 */
void executiveTest(qdisp::Executive::Ptr const& ex, SequentialInt &sequence,
                   SequentialInt &chunkId, std::string const& millisecs, int copies) {
    // Test class Executive::add
    // Modeled after ccontrol::UserQuery::submit()
    ResourceUnit ru;
    std::string chunkResultName = "mock";
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId.get());
    ccontrol::MergingHandler::Ptr mh = std::make_shared<ccontrol::MergingHandler>(cmr, infileMerger, chunkResultName);
    std::string msg = millisecs;
    RequesterVector rv;
    for (int j=0; j < copies; ++j) {
        rv.push_back(mh);
    }
    addMockRequests(ex, sequence, millisecs, rv);
}


/** This function is run in a separate thread to fail the test if it takes too long
 * for the jobs to complete.
 */
void timeoutFunc(util::Flag<bool>& flagDone, int millisecs) {
    LOGS_DEBUG("timeoutFunc");
    usleep(1000*millisecs);
    bool done = flagDone.get();
    LOGS_DEBUG("timeoutFunc sleep over millisecs=" << millisecs << " done=" << done);
    BOOST_REQUIRE(done == true);
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(Executive) {
    _log.setLevel(LOG_LVL_DEBUG);
    LOGS_DEBUG("Executive test 1");
    util::Flag<bool> done(false);
    // Modeled after ccontrol::UserQuery::submit()
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    int jobs = 0;
    // test single instance
    int millisInt = 200;
    std::thread timeoutT(&timeoutFunc, std::ref(done), millisInt*10);
    std::string millis(boost::lexical_cast<std::string>(millisInt));
    ++jobs;
    executiveTest(ex, sequence, chunkId, millis, 1);
    LOGS_DEBUG("jobs=" << jobs);
    ex->join();
    BOOST_CHECK(ex->getEmpty() == true);

    // test adding 4 jobs
    LOGS_DEBUG("Executive test 2");
    executiveTest(ex, sequence, chunkId, millis, 4);
    jobs += 4;
    ex->join();
    BOOST_CHECK(ex->getEmpty() == true);

    // Test that we can detect ex._empty == false.
    LOGS_DEBUG("Executive test 3");
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(false);
    executiveTest(ex, sequence, chunkId, millis, 5);
    jobs += 5;
    while (qdisp::XrdSsiServiceMock::_count.get() < jobs) {
        LOGS_DEBUG("waiting for _count(" << qdisp::XrdSsiServiceMock::_count.get()
                   << ") == jobs(" << jobs << ")");
        usleep(10000);
    }
    BOOST_CHECK(ex->getEmpty() == false);
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(true);
    ex->join();
    LOGS_DEBUG("ex->join() joined");
    BOOST_CHECK(ex->getEmpty() == true);
    done.exchange(true);
    timeoutT.join();
    LOGS_DEBUG("Executive test end");
}

BOOST_AUTO_TEST_CASE(MessageStore) {
    LOGS_DEBUG("MessageStore test start");
    qdisp::MessageStore ms;
    BOOST_CHECK(ms.messageCount() == 0);
    ms.addMessage(123, 456, "test1");
    std::string str("test2");
    ms.addMessage(124, -12, str);
    ms.addMessage(86, -12, "test3");
    BOOST_CHECK(ms.messageCount() == 3);
    BOOST_CHECK(ms.messageCount(-12) == 2);
    qdisp::QueryMessage qm = ms.getMessage(1);
    BOOST_CHECK(qm.chunkId == 124 && qm.code == -12 && str.compare(qm.description) == 0);
    LOGS_DEBUG("MessageStore test end");
}

BOOST_AUTO_TEST_CASE(QueryResource) {
    // Test that QueryResource::ProvisionDone detects NULL XrdSsiSesion
    LOGS_DEBUG("QueryResource test 1");
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    int jobId = 93;
    int chunkId = 14;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    auto jobDesc = makeMockJobDescription(ex, jobId, ru, "a message",
            std::make_shared<ccontrol::MergingHandler>(cmr, infileMerger, chunkResultName));
    qdisp::MarkCompleteFunc::Ptr mcf = std::make_shared<qdisp::MarkCompleteFunc>(ex, jobId);

    JobQueryTest::Ptr jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, mcf, true, nullptr, false);
    qdisp::QueryResource::Ptr r = jqTest->getQueryResource();
    r->ProvisionDone(nullptr);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state  == qdisp::JobStatus::PROVISION_NACK);

    // Session is used by JobQuery/Resource destructors, needs to have longer lifetime than
    // objects created below. To avoid lifetime issues we intentionally leak this instance.
    char buf[20];
    strcpy(buf, qdisp::XrdSsiSessionMock::getMockString());
    auto xsMock = new qdisp::XrdSsiSessionMock(buf);

    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, mcf, true, nullptr, false);
    r = jqTest->getQueryResource();
    r->ProvisionDone(xsMock);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state  == qdisp::JobStatus::REQUEST);
    BOOST_CHECK(jqTest->retryCalled == false);
}

BOOST_AUTO_TEST_CASE(QueryRequest) {
    LOGS_DEBUG("QueryRequest test");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and RetryTest (JobQuery child)
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    int jobId = 93;
    int chunkId = 14;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    auto jobDesc = makeMockJobDescription(ex, jobId, ru, "a message", respReq);
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();

    // Session is used by JobQuery/Resource destructors, needs to have longer lifetime than
    // objects created below. To avoid lifetime issues we intentionally leak this instance.
    char buf[20];
    strcpy(buf, "sessionMock");
    auto sessionMock = new qdisp::XrdSsiSessionMock(buf);

    JobQueryTest::Ptr jqTest =
        JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 1");
    // Test that ProcessResponse detects !isOk and retries.
    qdisp::QueryRequest::Ptr qrq = jqTest->getQueryRequest();
    XrdSsiRespInfo rInfo;
    rInfo.eNum = 123;
    rInfo.eMsg = "test_msg";
    qrq->ProcessResponse(rInfo, false);
    BOOST_CHECK(respReq->_code == -1);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(jqTest->retryCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 2");
    // Test that ProcessResponse detects XrdSsiRespInfo::isError.
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    int magicErrNum = 5678;
    rInfo.rType = XrdSsiRespInfo::isError;
    rInfo.eNum = magicErrNum;
    finishTest->finishCalled = false;
    qrq->ProcessResponse(rInfo, true);
    LOGS_DEBUG("respReq->_code=" << respReq->_code);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(respReq->_code == magicErrNum);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 3");
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    rInfo.rType = XrdSsiRespInfo::isStream;
    finishTest->finishCalled = false;
    qrq->ProcessResponse(rInfo, true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_ERROR_CORRUPT);
    BOOST_CHECK(finishTest->finishCalled);
    // The success case for ProcessResponse is probably best tested with integration testing.
    // Getting it work in a unit test requires replacing inline bool XrdSsiRequest::GetResponseData
    // or coding around that function call for the test. Failure of the path will have high visibility.
    LOGS_DEBUG("QueryRequest::ProcessResponseData test 1");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    const char* ts="abcdefghijklmnop";
    char dataBuf[50];
    strcpy(dataBuf, ts);
    qrq->ProcessResponseData(dataBuf, -7, true); // qrq deleted
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_NACK);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponseData test 2");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(dataBuf, ResponseHandlerTest::magic()+1, true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::MERGE_ERROR);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponseData test 3");
    finishTest->finishCalled = false;
    jqTest->retryCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, false, sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(dataBuf, ResponseHandlerTest::magic(), true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::COMPLETE);
    BOOST_CHECK(finishTest->finishCalled);
    BOOST_CHECK(!jqTest->retryCalled);
}

BOOST_AUTO_TEST_CASE(ExecutiveCancel) {
    // Test that all JobQueries are cancelled.
    LOGS_DEBUG("Check that executive squash");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and JobQueryTest child
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    int chunkId = 14;
    int first = 1;
    int last = 20;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobQuery::Ptr jq;
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(false); // Can't let jobs run or they are untracked before squash
    for (int jobId=first; jobId<=last; ++jobId) {
        auto jobDesc = makeMockJobDescription(ex, jobId, ru, "a message", respReq);
        auto jQuery = ex->add(jobDesc);
        jq = ex->getJobQuery(jobId);
        auto qRequest = jq->getQueryRequest();
        BOOST_CHECK(jq->isQueryCancelled() == false);
    }
    ex->squash();
    ex->squash(); // check that squashing twice doesn't cause issues.
    for (int jobId=first; jobId<=last; ++jobId) {
        jq = ex->getJobQuery(jobId);
        BOOST_CHECK(jq->isQueryCancelled() == true);
    }
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(true);
    usleep(250000); // Give mock threads a quarter second to complete.

    LOGS_DEBUG("Check that QueryResource and QueryRequest detect the cancellation of a job.");
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();
    int jobId = 7;
    respReq = std::make_shared<ResponseHandlerTest>();
    auto jobDesc = makeMockJobDescription(ex, jobId, ru, "a message", respReq);

    // Session is used by JobQuery/Resource destructors, needs to have longer lifetime than
    // objects created below. To avoid lifetime issues we intentionally leak this instance.
    char buf[20];
    strcpy(buf, "sessionMock");
    auto sessionMock = new qdisp::XrdSsiSessionMock(buf);

    qdisp::JobQuery::Ptr jqTest =
        JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest, true, sessionMock, true);
    auto resource = jqTest->getQueryResource();
    auto request = jqTest->getQueryRequest();
    BOOST_CHECK(request->isQueryRequestCancelled() == false);
    BOOST_CHECK(respReq->_processCancelCalled == false);
    jqTest->cancel();
    BOOST_CHECK(resource->isQueryCancelled() == true);
    BOOST_CHECK(request->isQueryCancelled() == true);
    BOOST_CHECK(request->isQueryRequestCancelled() == true);
    BOOST_CHECK(respReq->_processCancelCalled == true);

}

BOOST_AUTO_TEST_SUITE_END()



