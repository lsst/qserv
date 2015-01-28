// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
/// Test QservOss oss plugin.
#define BOOST_TEST_MODULE QservOss_1

// Third-party headers
#include "XrdSys/XrdSysLogger.hh"
#include "XrdOuc/XrdOucEnv.hh"

// Qserv headers
#include "xrdoss/QservOss.h"

// Boost unit test header
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using lsst::qserv::xrdoss::QservOss;

struct TestFixture {
    TestFixture(void) {
    };
    ~TestFixture(void) { };
    XrdSysLogger logger;
};

BOOST_FIXTURE_TEST_SUITE(QserOssSuite, TestFixture)

BOOST_AUTO_TEST_CASE(Test1) {
    QservOss*  oss = QservOss::getInstance();
    int result;
    char aPath[] = "/q/LSST/3838";
    char badPath[] = "/q/LSST/38";
    char tident[] = "user"; // tident = user ident in xrootd

    // Test the important calls.
    struct stat aStat;
    result = oss->Stat(aPath, &aStat);
    BOOST_CHECK_NE(result, -ENOTSUP);
    BOOST_CHECK((result == XrdOssOK) || (result == -ENOENT));

    result = oss->Stat(badPath, &aStat);
    BOOST_CHECK_NE(result, -ENOTSUP);
    BOOST_CHECK((result == XrdOssOK) || (result == -ENOENT));

    // Necessary but unimportant StatVS.
    result = oss->StatVS(NULL, NULL);
    BOOST_CHECK_EQUAL(result, -EEXIST);
    XrdOssVSInfo vsInfo;
    result = oss->StatVS(&vsInfo, NULL);
    BOOST_CHECK_EQUAL(result, XrdOssOK);

    // Test XrdOss overrides (stubs)
    boost::shared_ptr<XrdOssDF>  ossDf;
    ossDf.reset(oss->newDir(tident));
    BOOST_CHECK_NE(ossDf.get(), static_cast<XrdOssDF*>(NULL));
    ossDf.reset(oss->newFile(tident));
    BOOST_CHECK_NE(ossDf.get(),  static_cast<XrdOssDF*>(NULL));

    result = oss->Chmod(aPath, 0777, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);

    XrdOucEnv env;
    result = oss->Create(aPath, tident, 0777, env);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);

    result = oss->Init(&logger, NULL);
    BOOST_CHECK_EQUAL(result, 0);
    result = oss->Mkdir(aPath, 0777, 0, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);
    result = oss->Remdir(aPath, 0, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);
    result = oss->Truncate(aPath, 0, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);
    result = oss->Unlink(aPath, 0, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);
    result = oss->Rename(aPath, aPath, 0, NULL);
    BOOST_CHECK_EQUAL(result, -ENOTSUP);
}
BOOST_AUTO_TEST_SUITE_END()
