#include "XrdSfs/XrdSfsInterface.hh"

#define BOOST_TEST_MODULE MySqlFs_1
#include "boost/test/included/unit_test.hpp"

#include "XrdSys/XrdSysLogger.hh"

namespace test = boost::test_tools;

BOOST_AUTO_TEST_SUITE(MySqlFsSuite)

BOOST_AUTO_TEST_CASE(Make) {
    XrdSysLogger* lp = new XrdSysLogger;
    XrdSfsFileSystem* fs = XrdSfsGetFileSystem(0, lp, 0);
    BOOST_CHECK(fs != 0);
}

BOOST_AUTO_TEST_SUITE_END()
