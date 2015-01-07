// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

// Third-party headers
#include "boost/scoped_ptr.hpp"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysLogger.hh"

// Qserv headers
#include "xrdfs/MySqlFs.h"

// Boost unit test header
#define BOOST_TEST_MODULE MySqlFs_1
#include "boost/test/included/unit_test.hpp"


namespace test = boost::test_tools;

// Re-visit once new Xrootd interface is ready
//static XrdSysLogger logDest;

struct FsFixture {
    FsFixture(void) {
        // This is actually a pointer to a static instance, so it can't be put
        // into a smart pointer.
        //_fs = XrdSfsGetFileSystem(0, &logDest, 0);
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
    file->open("/query/314159", O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    // This message format is no longer supported.
    // Should fix for protobuf format and re-think this test.
    std::string query =
        "-- 42,99\n"
        "CREATE TABLE Result AS "
        "SELECT COUNT(*) FROM "
        "(SELECT * FROM Subchunks_314159.Object_314159_42 "
        "UNION "
        "SELECT * FROM Subchunks_314159.Object_314159_99) AS _Obj_Subchunks;";
    XrdSfsXferSize sz = file->write(0, query.c_str(), query.size());
    if (sz == -1) {
        int err;
        std::cerr << file->error.getErrText(err);
        std::cerr << ": " << strerror(err) << std::endl;
        BOOST_REQUIRE_NE(sz, -1);
    }
#if 0 // FIXME: reading requires a separate open-read-close transaction
    char result[4096];
    sz = file->read(0, result, sizeof(result));
    if (sz == -1) {
        int err;
        std::cerr << file->error.getErrText(err);
        std::cerr << ": " << strerror(err) << std::endl;
        BOOST_REQUIRE_NE(sz, -1);
    } else if (sz < 0) {
        BOOST_REQUIRE_GE(sz, 0);
    }
    std::cerr << std::string(result, sz) << std::endl;
#endif
    file->close();
}

BOOST_AUTO_TEST_SUITE_END()
