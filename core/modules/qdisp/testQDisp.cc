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

// External headers
#include "boost/make_shared.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MergingRequester.h"
#include "global/ResourceUnit.h"
#include "global/MsgReceiver.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qdisp/XrdSsiMocks.h"
#include "util/threadSafe.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

typedef util::Sequential<int> SequentialInt;
typedef std::vector<qdisp::ResponseRequester::Ptr> RequesterVector;

class ChunkMsgReceiverMock : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
        LOGF_INFO("chunkId=%1%, code=%2%, msg=%3%" % _chunkId % code % msg);
    }
    static boost::shared_ptr<ChunkMsgReceiverMock>
    newInstance(int chunkId) {
        boost::shared_ptr<ChunkMsgReceiverMock> r = boost::make_shared<ChunkMsgReceiverMock>();
        r->_chunkId = chunkId;
        return r;
    }
    int _chunkId;
};

/** Simple functor for testing that _retryfunc has been called.
 */
class RetryTest : public util::VoidCallable<void> {
public:
    typedef boost::shared_ptr<RetryTest> Ptr;
    RetryTest() : _retryCalled(false) {}
    virtual ~RetryTest() {}
    virtual void operator()() {
        _retryCalled = true;
        LOGF_INFO("_retryCalled=%1%" % _retryCalled);
    }
    bool _retryCalled;
};

/** Simple functor for testing _finishfunc.
 */
class FinishTest : public util::UnaryCallable<void, bool> {
public:
    typedef boost::shared_ptr<FinishTest> Ptr;
    FinishTest() : _finishCalled(false) {}
    virtual ~FinishTest() {}
    virtual void operator()(bool val) {
        _finishCalled = true;
        LOGF_INFO("_finishCalled=%1%" % _finishCalled);
    }
    bool _finishCalled;
};

/** Class for testing how many times CancelFunc is called.
 */
class CancelTest : public qdisp::ResponseRequester::CancelFunc {
public:
    CancelTest() : _count(0) {}
    virtual void operator()() {
        ++_count;
    }
    int _count;
};

/** Simple ResponseRequester for testing.
 */
class ResponseRequesterTest : public qdisp::ResponseRequester {
public:
    ResponseRequesterTest() : _code(0), _finished(false) {}
    virtual std::vector<char>& nextBuffer() {
        return _vect;
    }
    virtual bool flush(int bLen, bool last) {
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
    virtual std::ostream& print(std::ostream& os) const {
        return os;
    }
    static int magic() {return 8;}
    std::vector<char> _vect;
    std::string _msg;
    int _code;
    bool _finished;
};

/** Add dummy requests to an executive corresponding to the requesters
 */
void addFakeRequests(qdisp::Executive &ex, SequentialInt &sequence, std::string const& millisecs, RequesterVector& rv) {
    ResourceUnit ru;
    int copies = rv.size();
    std::vector<qdisp::Executive::Spec> s(copies);
    for(int j=0; j < copies; ++j) {
        s[j].resource = ru; // dummy
        s[j].request = millisecs; // Request = stringified milliseconds
        s[j].requester = rv[j];
        ex.add(sequence.incr(), s[j]); // ex.add() is not thread safe.
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
    boost::shared_ptr<rproc::InfileMerger> infileMerger;
    boost::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId.get());
    ccontrol::MergingRequester::Ptr mr = boost::make_shared<ccontrol::MergingRequester>(cmr, infileMerger, chunkResultName);
    std::string msg = millisecs;
    RequesterVector rv;
    for (int j=0; j < copies; ++j) {
        rv.push_back(mr);
    }
    addFakeRequests(ex, sequence, millisecs, rv);
}


/** This function is run in a separate thread to fail the test if it takes too long
 * for the jobs to complete.
 */
void timeoutFunc(util::Flag<bool>& flagDone, int millisecs) {
    LOGF_INFO("timeoutFunc");
    boost::this_thread::sleep(boost::posix_time::milliseconds(millisecs));
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
    qdisp::Executive::Config::Ptr conf = boost::make_shared<qdisp::Executive::Config>(str);
    boost::shared_ptr<qdisp::MessageStore> ms = boost::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    int jobs = 0;
    // test single instance
    int millisInt = 200;
    boost::thread timeoutT(&timeoutFunc, boost::ref(done), millisInt*10);
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
        boost::this_thread::sleep(boost::posix_time::milliseconds(10));
    }
    BOOST_CHECK(ex.getEmpty() == false);
    qdisp::XrdSsiServiceMock::_go.set(true);
    ex.join();
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
    LOGF_INFO("QueryResource test 1");
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = boost::make_shared<qdisp::Executive::Config>(str);
    boost::shared_ptr<qdisp::MessageStore> ms = boost::make_shared<qdisp::MessageStore>();
    qdisp::Executive ex(conf, ms);
    int refNum = 93;
    int chunkId = 14;
    ResourceUnit ru;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    boost::shared_ptr<rproc::InfileMerger> infileMerger;
    boost::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ccontrol::MergingRequester::Ptr mr =
            boost::make_shared<ccontrol::MergingRequester>(cmr, infileMerger, chunkResultName);
    qdisp::Executive::Spec s;
    s.resource = ru;
    s.request = "a message";
    s.requester = mr;
    qdisp::ExecStatus status(ru);
    boost::shared_ptr<RetryTest> retryTest = boost::make_shared<RetryTest>();
    qdisp::QueryResource* r = new qdisp::QueryResource(s.resource.path(), s.request, s.requester,
            qdisp::Executive::newNotifier(ex, refNum),
            retryTest, status);
    r->ProvisionDone(NULL);
    BOOST_CHECK(status.getInfo().state  == qdisp::ExecStatus::PROVISION_NACK);
    char buf[20];
    strcpy(buf, qdisp::XrdSsiSessionMock::getMockString(false));
    qdisp::XrdSsiSessionMock xsMockFalse(buf);
    r->ProvisionDone(&xsMockFalse);
    BOOST_CHECK(status.getInfo().state  == qdisp::ExecStatus::REQUEST_ERROR);
    BOOST_CHECK(retryTest->_retryCalled == true);
    // At this point, r has been deleted by QueryResource::ProvisionDone

    LOGF_INFO("QueryResource test 2");
    retryTest->_retryCalled = false;
    r = new qdisp::QueryResource(s.resource.path(), s.request, s.requester,
            qdisp::Executive::newNotifier(ex, refNum),
            retryTest, status);
    strcpy(buf, qdisp::XrdSsiSessionMock::getMockString(true));
    qdisp::XrdSsiSessionMock xsMockTrue(buf);
    r->ProvisionDone(&xsMockTrue);
    BOOST_CHECK(status.getInfo().state  == qdisp::ExecStatus::REQUEST);
    BOOST_CHECK(retryTest->_retryCalled == false);
    // At this point, r has been deleted by QueryResource::ProvisionDone
}

BOOST_AUTO_TEST_CASE(QueryRequest) {
    LOGF_INFO("QueryRequest test");
    ResourceUnit ru;
    char buf[20];
    strcpy(buf, "sessionMock");
    qdisp::XrdSsiSessionMock sessionMock(buf);
    boost::shared_ptr<FinishTest> finishTest = boost::make_shared<FinishTest>();
    boost::shared_ptr<RetryTest> retryTest = boost::make_shared<RetryTest>();
    //        boost::make_shared<RetryTest>(boost::ref(ex), refNum, boost::ref(s), boost::ref(status));
    qdisp::ExecStatus status(ru);
    boost::shared_ptr<ResponseRequesterTest> respReq = boost::make_shared<ResponseRequesterTest>();
    boost::shared_ptr<CancelTest> cancelTest = boost::make_shared<CancelTest>();
    respReq->registerCancel(cancelTest);

    LOGF_INFO("QueryRequest::ProcessResponse test 1");
    qdisp::QueryRequest *qrq =
            new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryTest, status);
    XrdSsiRespInfo rInfo;
    rInfo.eNum = 123;
    rInfo.eMsg = "test_msg";
    qrq->ProcessResponse(rInfo, false); // this causes qrq to delete itself.
    BOOST_CHECK(respReq->_code == -1);
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::RESPONSE_ERROR);
    BOOST_CHECK(retryTest->_retryCalled);

    LOGF_INFO("QueryRequest::ProcessResponse test 2");
    boost::shared_ptr<RetryTest> retryNull;
    qrq = new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryNull, status);
    int magicErrNum = 5678;
    rInfo.rType = XrdSsiRespInfo::isError;
    rInfo.eNum = magicErrNum;
    finishTest->_finishCalled = false;
    qrq->ProcessResponse(rInfo, true); // this causes qrq to delete itself.
    LOGF_INFO("respReq->_code=%1%" % respReq->_code);
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::RESPONSE_ERROR);
    BOOST_CHECK(respReq->_code == magicErrNum);
    BOOST_CHECK(finishTest->_finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponse test 3");
    qrq = new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryNull, status);
    rInfo.rType = XrdSsiRespInfo::isStream;
    finishTest->_finishCalled = false;
    qrq->ProcessResponse(rInfo, true); // this causes qrq to delete itself.
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::RESPONSE_DATA_ERROR_CORRUPT);
    BOOST_CHECK(finishTest->_finishCalled);
    // The success case for ProcessResponse is probably best tested with integration testing.
    // Getting it work in a unit test requires replacing inline bool XrdSsiRequest::GetResponseData
    // or coding around that function call for the test. Failure of the path will have high visibility.

    LOGF_INFO("QueryRequest::ProcessResponseData test 1");
    finishTest->_finishCalled = false;
    qrq = new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryNull, status);
    const char* ts="abcdefghijklmnop";
    char dataBuf[50];
    strcpy(dataBuf, ts);
    qrq->ProcessResponseData(dataBuf, -7, true); // qrq deleted
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::RESPONSE_DATA_NACK);
    BOOST_CHECK(finishTest->_finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponseData test 2");
    finishTest->_finishCalled = false;
    qrq = new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryNull, status);
    qrq->ProcessResponseData(dataBuf, ResponseRequesterTest::magic()+1, true); // qrq deleted
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::MERGE_ERROR);
    BOOST_CHECK(finishTest->_finishCalled);

    LOGF_INFO("QueryRequest::ProcessResponseData test 3");
    finishTest->_finishCalled = false;
    retryTest->_retryCalled = false;
    qrq = new qdisp::QueryRequest(&sessionMock, "mock", respReq, finishTest, retryTest, status);
    qrq->ProcessResponseData(dataBuf, ResponseRequesterTest::magic(), true); // qrq deleted
    BOOST_CHECK(status.getInfo().state == qdisp::ExecStatus::COMPLETE);
    BOOST_CHECK(finishTest->_finishCalled);
    BOOST_CHECK(!retryTest->_retryCalled);
}

BOOST_AUTO_TEST_CASE(ResponseRequester) {
    LOGF_INFO("ResponseRequester test 1");
    boost::shared_ptr<ResponseRequesterTest> respReq = boost::make_shared<ResponseRequesterTest>();
    boost::shared_ptr<CancelTest> cancelTest = boost::make_shared<CancelTest>();
    respReq->registerCancel(cancelTest);
    // The cancel function should only be called once.
    BOOST_CHECK(cancelTest->_count == 0);
    respReq->cancel();
    BOOST_CHECK(cancelTest->_count == 1);
    respReq->cancel();
    BOOST_CHECK(cancelTest->_count == 1);
}

BOOST_AUTO_TEST_CASE(TransactionSpec) {
    // TransactionSpec is obsolete, it and this block should probably go away.
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(Squash)

class CancellableRequester : public qdisp::ResponseRequester {
public:
    using ResponseRequester::CancelFunc;

    typedef boost::shared_ptr<CancellableRequester> Ptr;
    CancellableRequester()
        : _cancelCalls(0) {
        reset();
    }
    virtual ~CancellableRequester() {}

    virtual std::vector<char>& nextBuffer() {
        return _buffer;
    }

    virtual bool flush(int bLen, bool last) {
        _flushedBytes += bLen;
        if(_receivedLast) {
            throw std::runtime_error("Duplicate last");
        }
        _receivedLast = last;
        return true;
    }
    virtual void errorFlush(std::string const& msg, int code) {
        _lastError.msg = msg;
        _lastError.code = code;
    }

    virtual bool finished() const { return false; }
    virtual bool reset() {
        _flushedBytes = 0;
        _receivedLast = false;
        _lastError = Error();
        return true;
    }

    virtual std::ostream& print(std::ostream& os) const {
        return os << "CancellableRequester";
    }

    virtual Error getError() const { return _lastError; }

    virtual void registerCancel(boost::shared_ptr<CancelFunc> cancelFunc) {
        throw std::runtime_error("Unexpected registerCancel() call");
    }

    virtual void cancel() { _cancelCalls += 1; }
    virtual bool cancelled() { return _cancelCalls > 0; }

    // Leave "public" to allow test checking.
    std::vector<char> _buffer;
    int _flushedBytes;
    bool _receivedLast;
    int _cancelCalls;
    Error _lastError;
};

BOOST_AUTO_TEST_CASE(ExecutiveSquash) {
    qdisp::Executive ex(boost::make_shared<qdisp::Executive::Config>(0,0),
                        boost::make_shared<qdisp::MessageStore>());
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    int jobs = 0;
    const int CHUNK_COUNT=4;

    int millisInt = 200;
    std::string millis(boost::lexical_cast<std::string>(millisInt));
    jobs += CHUNK_COUNT;
    RequesterVector rv;
    for (int j=0; j < CHUNK_COUNT; ++j) {
        rv.push_back(boost::make_shared<CancellableRequester>());
    }
    addFakeRequests(ex, sequence, millis, rv);
    LOGF_INFO("jobs=%1%" % jobs);
    ex.requestSquash(2); // Squash one of the items.
    ex.join();
    BOOST_CHECK(ex.getEmpty() == true);
    // See if the requesters got the cancellation message.
    int rCancels = 0;
    for (int j=0; j < CHUNK_COUNT; ++j) {
        int c = dynamic_cast<CancellableRequester*>(rv[j].get())->_cancelCalls;
        if (c > 0) { ++rCancels; }
    }
    BOOST_CHECK_EQUAL(rCancels, 1);

    for (int j=0; j < CHUNK_COUNT; ++j) { // Reset rv
        rv[j] = boost::make_shared<CancellableRequester>();
    }
    addFakeRequests(ex, sequence, millis, rv);
    LOGF_INFO("jobs=%1%" % jobs);
    ex.squash();
    ex.join();
    BOOST_CHECK(ex.getEmpty() == true);
    // See if the requesters got the cancellation message.
    rCancels = 0;
    for (int j=0; j < CHUNK_COUNT; ++j) {
        int c = dynamic_cast<CancellableRequester*>(rv[j].get())->_cancelCalls;
        if (c > 0) { ++rCancels; }
    }
    BOOST_CHECK_EQUAL(rCancels, 4);

}

BOOST_AUTO_TEST_SUITE_END()


