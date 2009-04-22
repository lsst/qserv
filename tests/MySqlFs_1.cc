#include "XrdSfs/XrdSfsInterface.hh"

#define BOOST_TEST_MODULE MySqlFs_1
#include "boost/test/included/unit_test.hpp"

#include "XrdSys/XrdSysLogger.hh"
#include <cerrno>

namespace test = boost::test_tools;

struct FsFixture {
    FsFixture(void) {
        // This ends up in a static, so it can't be put into a smart pointer.
        _lp = new XrdSysLogger;
        // This is actually a pointer to a static instance, so it can't be put
        // into a smart pointer, either.
        _fs = XrdSfsGetFileSystem(0, _lp, 0);
    };
    ~FsFixture(void) { };

    XrdSysLogger* _lp;
    XrdSfsFileSystem* _fs;
};

BOOST_FIXTURE_TEST_SUITE(MySqlFsSuite, FsFixture)

BOOST_AUTO_TEST_CASE(Make) {
    BOOST_CHECK(_fs != 0);
}

BOOST_AUTO_TEST_CASE(Unimplemented) {
    XrdOucErrInfo outErr;
    BOOST_CHECK_EQUAL(_fs->chmod("foo", 0644, outErr, 0, 0), SFS_ERROR);
    XrdSfsFileExistence exists;
    BOOST_CHECK_EQUAL(_fs->exists("foo", exists, outErr, 0, 0), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->fsctl(0, "foo", outErr, 0), SFS_ERROR);
    char buf[80];
    BOOST_CHECK_EQUAL(_fs->getStats(buf, sizeof(buf)), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->mkdir("foo", 0755, outErr, 0, 0), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->rem("foo", outErr, 0, 0), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->remdir("foo", outErr, 0, 0), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->rename("foo", "bar", outErr, 0, 0, 0), SFS_ERROR);
    mode_t m;
    BOOST_CHECK_EQUAL(_fs->stat("foo", m, outErr, 0, 0), SFS_ERROR);
    BOOST_CHECK_EQUAL(_fs->truncate("foo", 0, outErr, 0, 0), SFS_ERROR);
}

BOOST_AUTO_TEST_SUITE_END()
