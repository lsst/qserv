#include "XrdSfs/XrdSfsInterface.hh"

#define BOOST_TEST_MODULE MySqlFs_2
#include "boost/test/included/unit_test.hpp"

#include "XrdSys/XrdSysLogger.hh"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"

#include "lsst/qserv/worker/ResultTracker.h"

namespace test = boost::test_tools;
namespace qWorker = lsst::qserv::worker;

struct TrackerFixture {
    struct StrCallable {
	StrCallable(std::string& s) : isNotified(false), val(s) {}
	void operator()(std::string const& s) {
	    isNotified = true;
	    val.assign(s);
	}
	std::string& val;
	bool isNotified;
    };

};

BOOST_FIXTURE_TEST_SUITE(ResultTrackerSuite, TrackerFixture)

BOOST_AUTO_TEST_CASE(IntKey) {
    qWorker::ResultTracker<int, std::string> rt;
    BOOST_CHECK(rt.getSignalCount() == 0);
    BOOST_CHECK(rt.getNewsCount() == 0);
    std::string msg;
    StrCallable sc(msg);
    rt.listenOnce(5, sc);
		 
}

BOOST_AUTO_TEST_SUITE_END()
