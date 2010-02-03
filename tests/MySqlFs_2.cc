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

BOOST_AUTO_TEST_SUITE_END()
