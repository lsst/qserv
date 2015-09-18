// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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

// Boost unit test header
#define BOOST_TEST_MODULE Qdisp_1
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MergingRequester.h"
#include "global/ResourceUnit.h"
#include "global/MsgReceiver.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "qdisp/MessageStore.h"
#include "qdisp/XrdSsiMocks.h"
#include "util/threadSafe.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

typedef util::Sequential<int> SequentialInt;
typedef std::vector<qdisp::ResponseHandler::Ptr> RequesterVector;

class ChunkMsgReceiverMock : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
        LOGF_INFO("Mock::operator() chunkId=%1%, code=%2%, msg=%3%" % _chunkId % code % msg);
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
    JobQueryTest(qdisp::Executive* executive,
                 qdisp::JobDescription const& jobDescription,
                 qdisp::JobStatus::Ptr jobStatus,
                 qdisp::MarkCompleteFunc::Ptr markCompleteFunc)
        : qdisp::JobQuery{executive, jobDescription, jobStatus, markCompleteFunc} {}

    virtual ~JobQueryTest() {}
    virtual bool runJob() override {
        retryCalled = true;
        LOGF_INFO("_retryCalled=%1%" % retryCalled);
        return true;
    }
    bool retryCalled {false};

    // Create a fresh JobQueryTest instance. If you're making this to get a QueryRequestObject,
    // set createQueryRequest=true and pass an xsSession pointer.
    // set createQueryResource=true to get a QueryResource.
    // Special ResponseHandlers need to be defined in JobDescription.
    static JobQueryTest::Ptr getJobQueryTest(
            qdisp::Executive* executive, qdisp::JobDescription jobDesc,
            qdisp::MarkCompleteFunc::Ptr markCompleteFunc,
            bool createQueryResource, XrdSsiSession* xsSession, bool createQueryRequest) {
        qdisp::JobStatus::Ptr status(new qdisp::JobStatus());
        std::shared_ptr<JobQueryTest> jqTest(new JobQueryTest(executive, jobDesc, status, markCompleteFunc));
        jqTest->setup();
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
        LOGF_INFO("_finishCalled=%1%" % finishCalled);
    }
    bool finishCalled {false};
};

/** Simple ResponseHandler for testing.
 */
class ResponseHandlerTest : public qdisp::ResponseHandler {
public:
    ResponseHandlerTest() : _code(0), _finished(false), _processCancelCalled(false) {}
    virtual std::vector<char>& nextBuffer() {
        return _vect;
    }
    virtual bool flush(int bLen, bool& last) {
        return bLen == magic();
    }
    virtual void errorFlush(std::string const& msg, int code) {
        _msg = msg;
        _code = code;
    }
    virtual bool finished() const {
        return _finished;
    }
    virtual bool reset() {
        return true;
    }
    virtual qdisp::ResponseHandler::Error getError() const {
        return qdisp::ResponseHandler::Error(-1, "testQDisp Error");
    }
    virtual std::ostream& print(std::ostream& os) const {
        return os;
    }
    virtual void processCancel() {
        _processCancelCalled = true;
    }

    static int magic() {return 8;}
    std::vector<char> _vect;
    std::string _msg;
    int _code;
    bool _finished;
    bool _processCancelCalled;
};

/** Add dummy requests to an executive corresponding to the requesters
 */
void addFakeRequests(qdisp::Executive &ex, SequentialInt &sequence, std::string const& millisecs, RequesterVector& rv) {
    ResourceUnit ru;
    int copies = rv.size();
    std::vector<std::shared_ptr<qdisp::JobDescription>> s(copies);
    for(int j=0; j < copies; ++j) {
        // The job copies the JobDescription.
        qdisp::JobDescription job(sequence.incr(),
                ru,        // dummy
                millisecs, // Request = stringified milliseconds
                rv[j]);
        ex.add(job); // ex.add() is not thread safe.
    }
}

/** Start adds 'copies' number of test requests that each sleep for 'millisecs' time
 * before signaling to 'ex' that they are done.
 * Returns time to complete in seconds.
 */
void executiveTest(qdisp::Executive &ex, SequentialInt &sequence, SequentialInt &chunkId, std::string const& millisecs, int copies) {
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
    addFakeRequests(ex, sequence, millisecs, rv);
}


/** This function is run in a separate thread to fail the test if it takes too long
 * for the jobs to complete.
 */
void timeoutFunc(util::Flag<bool>& flagDone, int millisecs) {
    LOGF_INFO("timeoutFunc");
    usleep(1000*millisecs);
    bool done = flagDone.get();
    LOGF_INFO("timeoutFunc sleep over millisecs=%1% done=%2%" % millisecs % done);
    BOOST_REQUIRE(done == true);
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(Executive) {
    LOGF_INFO("Executive test 1");
    util::Flag<bool> done(false);
    // Modeled after ccontrol::UserQuery::submit()
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    int jobs = 0;
    // test single instance
    int millisInt = 200;
    std::thread timeoutT(&timeoutFunc, std::ref(done), millisInt*10);
    std::string millis(boost::lexical_cast<std::string>(millisInt));
    ++jobs;
    executiveTest(ex, sequence, chunkId, millis, 1);
    LOGF_INFO("jobs=%1%" % jobs);
    ex.join();
    BOOST_CHECK(ex.getEmpty() == true);

    // test adding 4 jobs
    LOGF_INFO("Executive test 2");
    executiveTest(ex, sequence, chunkId, millis, 4);
    jobs += 4;
    ex.join();
    BOOST_CHECK(ex.getEmpty() == true);

    // Test that we can detect ex._empty == false.
    LOGF_INFO("Executive test 3");
    qdisp::XrdSsiServiceMock::_go.set(false);
    executiveTest(ex, sequence, chunkId, millis, 5);
    jobs += 5;
    while (qdisp::XrdSsiServiceMock::_count.get() < jobs) {
        LOGF_INFO("waiting for _count(%1%) == jobs(%2%)" % qdisp::XrdSsiServiceMock::_count.get() % jobs);
        usleep(10000);
    }
    BOOST_CHECK(ex.getEmpty() == false);
    qdisp::XrdSsiServiceMock::_go.set(true);
    ex.join();
    LOGF_DEBUG("ex.join() joined");
    BOOST_CHECK(ex.getEmpty() == true);
    done.set(true);
    timeoutT.join();
    LOGF_INFO("Executive test end");
}

BOOST_AUTO_TEST_CASE(MessageStore) {
    LOGF_INFO("MessageStore test start");
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
    LOGF_INFO("MessageStore test end");
}

BOOST_AUTO_TEST_CASE(QueryResource) {
    // Test that QueryResource::ProvisionDone detects NULL XrdSsiSesion
    LOGF_INFO("QueryResource test 1");
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    int jobId = 93;
    int chunkId = 14;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    qdisp::JobDescription jobDesc(jobId, ru, "a message",
            std::make_shared<ccontrol::MergingHandler>(cmr, infileMerger, chunkResultName));
    qdisp::MarkCompleteFunc::Ptr mcf = std::make_shared<qdisp::MarkCompleteFunc>(&ex, jobId);

    JobQueryTest::Ptr jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, mcf, true, nullptr, false);
    qdisp::QueryResource::Ptr r = jqTest->getQueryResource();
    r->ProvisionDone(nullptr);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state  == qdisp::JobStatus::PROVISION_NACK);

    char buf[20];
    strcpy(buf, qdisp::XrdSsiSessionMock::getMockString());
    qdisp::XrdSsiSessionMock xsMock(buf);
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, mcf, true, nullptr, false);
    r = jqTest->getQueryResource();
    r->ProvisionDone(&xsMock);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state  == qdisp::JobStatus::REQUEST);
    BOOST_CHECK(jqTest->retryCalled == false);
}

BOOST_AUTO_TEST_CASE(QueryRequest) {
    LOGF_INFO("QueryRequest test");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and RetryTest (JobQuery child)
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    int jobId = 93;
    int chunkId = 14;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();
    char buf[20];
    strcpy(buf, "sessionMock");
    qdisp::XrdSsiSessionMock sessionMock(buf);
    JobQueryTest::Ptr jqTest =
        JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);

    LOGF_INFO("QueryRequest::ProcessResponse test 1");
    // Test that ProcessResponse detects !isOk and retries.
    qdisp::QueryRequest::Ptr qrq = jqTest->getQueryRequest();
    XrdSsiRespInfo rInfo;
    rInfo.eNum = 123;
    rInfo.eMsg = "test_msg";
    qrq->ProcessResponse(rInfo, false);
    BOOST_CHECK(respReq->_code == -1);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(jqTest->retryCalled);

    LOGF_INFO("QueryRequest::ProcessResponse test 2");
    // Test that ProcessResponse detects XrdSsiRespInfo::isError.
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);
    qrq = jqTest->getQueryRequest();
    int magicErrNum = 5678;
    rInfo.rType = XrdSsiRespInfo::isError;
    rInfo.eNum = magicErrNum;
    finishTest->finishCalled = false;
    qrq->ProcessResponse(rInfo, true);
    LOGF_INFO("respReq->_code=%1%" % respReq->_code);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(respReq->_code == magicErrNum);
    BOOST_CHECK(finishTest->finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponse test 3");
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);
    qrq = jqTest->getQueryRequest();
    rInfo.rType = XrdSsiRespInfo::isStream;
    finishTest->finishCalled = false;
    qrq->ProcessResponse(rInfo, true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_ERROR_CORRUPT);
    BOOST_CHECK(finishTest->finishCalled);
    // The success case for ProcessResponse is probably best tested with integration testing.
    // Getting it work in a unit test requires replacing inline bool XrdSsiRequest::GetResponseData
    // or coding around that function call for the test. Failure of the path will have high visibility.
    LOGF_INFO("QueryRequest::ProcessResponseData test 1");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);
    qrq = jqTest->getQueryRequest();
    const char* ts="abcdefghijklmnop";
    char dataBuf[50];
    strcpy(dataBuf, ts);
    qrq->ProcessResponseData(dataBuf, -7, true); // qrq deleted
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_NACK);
    BOOST_CHECK(finishTest->finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponseData test 2");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(dataBuf, ResponseHandlerTest::magic()+1, true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::MERGE_ERROR);
    BOOST_CHECK(finishTest->finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponseData test 3");
    finishTest->finishCalled = false;
    jqTest->retryCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, false, &sessionMock, true);
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(dataBuf, ResponseHandlerTest::magic(), true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::COMPLETE);
    BOOST_CHECK(finishTest->finishCalled);
    BOOST_CHECK(!jqTest->retryCalled);
}

BOOST_AUTO_TEST_CASE(ExecutiveCancel) {
    // Test that all JobQueries are cancelled.
    LOGF_INFO("Check that executive squash");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and JobQueryTest child
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    int chunkId = 14;
    int first = 1;
    int last = 20;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobQuery::Ptr jq;
    for (int jobId=first; jobId<=last; ++jobId) {
        qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);
        ex.add(jobDesc);
        jq = ex.getJobQuery(jobId);
        auto qRequest = jq->getQueryRequest();
        BOOST_CHECK(jq->isCancelled() == false);
    }
    ex.squash();
    ex.squash(); // check that squashing twice doesn't cause issues.
    for (int jobId=first; jobId<=last; ++jobId) {
        jq = ex.getJobQuery(jobId);
        BOOST_CHECK(jq->isCancelled() == true);
    }
    ex.join(); // XrdSsiMock doesn't pay attention to cancel, need to wait for all to finish.

    LOGF_INFO("Check that QueryResource and QueryRequest detect the cancellation of a job.");
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();
    int jobId = 7;
    respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);
    char buf[20];
    strcpy(buf, "sessionMock");
    qdisp::XrdSsiSessionMock sessionMock(buf);
    qdisp::JobQuery::Ptr jqTest =
        JobQueryTest::getJobQueryTest(&ex, jobDesc, finishTest, true, &sessionMock, true);
    auto resource = jqTest->getQueryResource();
    auto request = jqTest->getQueryRequest();
    BOOST_CHECK(resource->isCancelled() == false);
    BOOST_CHECK(request->isCancelled() == false);
    BOOST_CHECK(respReq->_processCancelCalled == false);
    jqTest->cancel();
    BOOST_CHECK(resource->isCancelled() == true);
    BOOST_CHECK(request->isCancelled() == true);
    BOOST_CHECK(respReq->_processCancelCalled == true);

}

BOOST_AUTO_TEST_SUITE_END()



