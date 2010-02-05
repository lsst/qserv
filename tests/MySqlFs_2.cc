#include "XrdSfs/XrdSfsInterface.hh"

#define BOOST_TEST_MODULE MySqlFs_2
#include "boost/test/included/unit_test.hpp"

#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysError.hh"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/make_shared.hpp"

#include "lsst/qserv/worker/ResultTracker.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/MySqlFsFile.h"

namespace test = boost::test_tools;
namespace qWorker = lsst::qserv::worker;
using boost::make_shared;

static XrdSysLogger logDest;
static XrdSysError errDest(&logDest);

// For chunk 9980, subchunks 1,3 (tuson26 right now)
std::string queryNonMagic =
    "-- SUBCHUNKS: 1,3\n"
    "CREATE TABLE Result AS "
    "SELECT COUNT(*) FROM "
    "SELECT * from (SELECT * FROM Subchunks_9980.Object_9980_1 "
    "UNION "
    "SELECT * FROM Subchunks_9980.Object_9980_3) AS _Obj_Subchunks;"; 
std::string query(queryNonMagic + std::string(4, '\0')); // Force magic EOF
std::string queryHash = qWorker::hashQuery(query.c_str(), query.size());
std::string queryResultPath = "/result/"+queryHash;

struct TrackerFixture {
    TrackerFixture() :
	invokeFile(&errDest, "qsmaster", make_shared<AddCallbackFunc>()),
	resultFile(&errDest, "qsmaster") {}
    class StrCallable {
    public:
	StrCallable(std::string& s) : isNotified(false), val(s) {}
	void operator()(std::string const& s) {
	    isNotified = true;
	    val.assign(s);
	}
	std::string& val;
	bool isNotified;
    };
    class Listener {
    public:
	Listener(std::string const& filename) :_filename(filename) {}
	virtual void operator()(qWorker::ErrorPair const& p) {
	    std::cout << "notification received for file " 
		      << _filename << std::endl;
	}
	std::string _filename;
    };
    class AddCallbackFunc : public qWorker::AddCallbackFunction {
    public:
	typedef boost::shared_ptr<AddCallbackFunction> Ptr;
	AddCallbackFunc() {}

	virtual ~AddCallbackFunc() {}
	virtual void operator()(XrdSfsFile& caller, 
				std::string const& filename) {
	    qWorker::QueryRunner::getTracker().listenOnce(filename, Listener(filename));
	}
    };
    
    qWorker::QueryRunner::Tracker& getTracker() { // alias.
	return qWorker::QueryRunner::getTracker();
    }
    
    qWorker::MySqlFsFile invokeFile;
    qWorker::MySqlFsFile resultFile;
    int lastResult;
};


BOOST_FIXTURE_TEST_SUITE(ResultTrackerSuite, TrackerFixture)

BOOST_AUTO_TEST_CASE(IntKey) {
    qWorker::ResultTracker<int, std::string> rt;
    BOOST_CHECK(rt.getSignalCount() == 0);
    BOOST_CHECK(rt.getNewsCount() == 0);
    std::string msg;
    StrCallable sc(msg);
    rt.listenOnce(5, sc);
    BOOST_CHECK(rt.getSignalCount() == 1);
    BOOST_CHECK(rt.getNewsCount() == 0);
    rt.notify(4,"no!");
    BOOST_CHECK(rt.getNewsCount() == 1);
    BOOST_CHECK(rt.getSignalCount() == 2);
    BOOST_CHECK(msg.size() == 0);
    rt.notify(5, "five");
    BOOST_CHECK(rt.getNewsCount() == 2);
    BOOST_CHECK(rt.getSignalCount() == 2);
    BOOST_CHECK(msg.size() == 4);
    std::string msg2;
    StrCallable sc2(msg2);
    rt.listenOnce(4, sc2);
    BOOST_CHECK(rt.getNewsCount() == 2);
    BOOST_CHECK(rt.getSignalCount() == 2);
    BOOST_CHECK(msg2.size() == 3);		 
}

BOOST_AUTO_TEST_CASE(QueryAttemptCombo) {
    // params: filename, openMode(ignored), createMode(ignored), 
    // clientSecEntity(ignored), opaque(ignored)

    lastResult = invokeFile.open("/query/9980",0,0,0,0);
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    lastResult = invokeFile.write(0, query.c_str(), query.size());
    BOOST_CHECK_EQUAL(lastResult, query.size());
    int pos = 0;
    const int blocksize = 1024;
    char contents[blocksize+1];
    while(1) {
	lastResult = invokeFile.read(pos, contents, blocksize);
	BOOST_CHECK(lastResult >= 0);
	if(lastResult >= 0) {
	    pos += blocksize;
	    contents[lastResult] = '\0';
	    std::cout << "recv("<< lastResult 
		      << "):" << contents << std::endl;
	} else {
	    std::cout << "recv error("<< lastResult 
		      << "):" << std::endl;
	}
	if(lastResult < blocksize)
	    break;
    }
    
    lastResult = invokeFile.close();
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    

}
#if 0
BOOST_AUTO_TEST_CASE(QueryAttemptCombo) {
    lastResult = invokeFile.open("/query2/9980",0,0,0,0);
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    lastResult = invokeFile.write(0, query.c_str(), query.size());
    BOOST_CHECK_EQUAL(lastResult, query.size());
    lastResult = invokeFile.close();
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    

    lastResult = invokeFile.open(queryResultPath.c_str(),0,0,0,0);
    BOOST_CHECK_EQUAL(lastResult, SFS_STARTED);
    qWorker::ErrorPtr p;
    while(1) {
	p = getTracker().getNews(queryResultPath);
	if(p.get() != 0) break;
	sleep(1);
    }
    
}
#endif
BOOST_AUTO_TEST_SUITE_END()
