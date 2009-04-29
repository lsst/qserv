#include "XrdSfs/XrdSfsInterface.hh"

#define BOOST_TEST_MODULE MySqlFs_1
#include "boost/test/included/unit_test.hpp"

#include "XrdSys/XrdSysLogger.hh"
#include <cerrno>
#include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"

namespace test = boost::test_tools;

static XrdSysLogger logDest;

struct FsFixture {
    FsFixture(void) {
        // This is actually a pointer to a static instance, so it can't be put
        // into a smart pointer.
        _fs = XrdSfsGetFileSystem(0, &logDest, 0);
    };
    ~FsFixture(void) { };

    XrdSfsFileSystem* _fs;
};

BOOST_FIXTURE_TEST_SUITE(MySqlFsSuite, FsFixture)

BOOST_AUTO_TEST_CASE(FsUnimplemented) {
    BOOST_CHECK(_fs != 0);
    XrdOucErrInfo outErr;
    BOOST_CHECK_EQUAL(_fs->chmod("foo", 0644, outErr), SFS_ERROR);
    int err;
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    XrdSfsFileExistence exists;
    BOOST_CHECK_EQUAL(_fs->exists("foo", exists, outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->fsctl(0, "foo", outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    char buf[80];
    BOOST_CHECK_EQUAL(_fs->getStats(buf, sizeof(buf)), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->mkdir("foo", 0755, outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->rem("foo", outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->remdir("foo", outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->rename("foo", "bar", outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    mode_t m;
    BOOST_CHECK_EQUAL(_fs->stat("foo", m, outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
    BOOST_CHECK_EQUAL(_fs->truncate("foo", 0, outErr), SFS_ERROR);
    BOOST_CHECK(strcmp(outErr.getErrText(err), "Operation not supported") == 0);
    BOOST_CHECK_EQUAL(err, ENOTSUP);
}

BOOST_AUTO_TEST_CASE(Directory) {
    BOOST_CHECK(_fs != 0);
    boost::scoped_ptr<XrdSfsDirectory> dir(_fs->newDir());
    BOOST_CHECK(dir.get() != 0);

    BOOST_CHECK_EQUAL(dir->open("/tmp"), SFS_ERROR);
    BOOST_CHECK(dir->nextEntry() == 0);
    BOOST_CHECK_EQUAL(dir->close(), SFS_ERROR);
    BOOST_CHECK(dir->FName() == 0);
}

BOOST_AUTO_TEST_CASE(FileUnimplemented) {
    BOOST_CHECK(_fs != 0);
    boost::scoped_ptr<XrdSfsFile> file(_fs->newFile());
    BOOST_CHECK(file.get() != 0);
    XrdOucErrInfo outErr;
    BOOST_CHECK_EQUAL(file->fctl(0, "x", outErr), SFS_ERROR);
    off_t size;
    BOOST_CHECK_EQUAL(file->getMmap(0, size), SFS_ERROR);
    BOOST_CHECK_EQUAL(file->read(static_cast<XrdSfsAio*>(0)), SFS_ERROR);
    BOOST_CHECK_EQUAL(file->write(static_cast<XrdSfsAio*>(0)), SFS_ERROR);
    BOOST_CHECK_EQUAL(file->sync(), SFS_ERROR);
    BOOST_CHECK_EQUAL(file->sync(static_cast<XrdSfsAio*>(0)), SFS_ERROR);
    struct stat st;
    BOOST_CHECK_EQUAL(file->stat(&st), SFS_ERROR);
    BOOST_CHECK_EQUAL(file->truncate(0), SFS_ERROR);
    char buf[4];
    int ret;
    BOOST_CHECK_EQUAL(file->getCXinfo(buf, ret), SFS_ERROR);
}

BOOST_AUTO_TEST_CASE(File) {
    BOOST_CHECK(_fs != 0);
    boost::scoped_ptr<XrdSfsFile> file(_fs->newFile());
    BOOST_CHECK(file.get() != 0);
    file->open("314159", O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    std::string query = "-- 42,99\nSELECT COUNT(*) FROM Object;";
    XrdSfsXferSize sz = file->write(0, query.c_str(), query.size());
    BOOST_CHECK_EQUAL(sz, -1);
    int err;
    std::cerr << file->error.getErrText(err);
    std::cerr << ": " << strerror(err) << std::endl;
    char result[4096];
    sz = file->read(0, result, sizeof(result)); 
    BOOST_CHECK_EQUAL(sz, -1);
    std::cerr << file->error.getErrText(err);
    std::cerr << ": " << strerror(err) << std::endl;
    file->close();
}

BOOST_AUTO_TEST_SUITE_END()
