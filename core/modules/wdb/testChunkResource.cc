// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  /**
  * @brief Simple testing for class ChunkResource
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>

// Qserv headers
#include "wdb/ChunkResource.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkResource_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using lsst::qserv::wdb::FakeBackend;
using lsst::qserv::wdb::ChunkResource;
using lsst::qserv::wdb::ChunkResourceMgr;

struct Fixture {

    Fixture() {
        for(int i=11; i<16; ++i) {
            subchunks.push_back(i);
        }
        thedb = "Snowden";
        tables.emplace(lsst::qserv::DbTable(thedb, "hello"));
        tables.emplace(lsst::qserv::DbTable(thedb, "goodbye"));
    }
    ~Fixture() {}

    std::vector<int> subchunks;
    std::string thedb;
    lsst::qserv::DbTableSet tables;
};


BOOST_FIXTURE_TEST_SUITE(All, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {

    auto backend = std::make_shared<FakeBackend>();
    std::shared_ptr<ChunkResourceMgr> crm = ChunkResourceMgr::newMgr(backend);
    BOOST_CHECK(backend->fakeSet.empty());
    BOOST_CHECK(crm->getRefCount(thedb, 12345) == 0);
    {
        ChunkResource cr12345(crm->acquire(thedb, 12345, tables));
        std::cout << "backend->fakeSet.size() ==" << backend->fakeSet.size() << std::endl;
        BOOST_CHECK(backend->fakeSet.size() == 0);

        subchunks = {28, 33};
        ChunkResource cr12345sub(crm->acquire(thedb, 12345, tables, subchunks));
        BOOST_CHECK(backend->fakeSet.size() == 4); // 2 tables * 2 subchunks
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 2);
        {
            ChunkResource foo = cr12345;
            BOOST_CHECK(crm->getRefCount(thedb, 12345) == 3);

            ChunkResource bar(cr12345sub);
            BOOST_CHECK(crm->getRefCount(thedb, 12345) == 4);
            BOOST_CHECK(backend->fakeSet.size() == 4);
        }
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 2);
        {
            ChunkResource foo = cr12345sub;
            BOOST_CHECK(crm->getRefCount(thedb, 12345) == 3);

            ChunkResource bar(cr12345);
            BOOST_CHECK(crm->getRefCount(thedb, 12345) == 4);
            BOOST_CHECK(backend->fakeSet.size() == 4);
        }
        // now, these resources should be in acquired
        BOOST_CHECK(backend->fakeSet.size() == 4);
    }
    // Now, these resources should be freed.
    BOOST_CHECK(crm->getRefCount(thedb, 12345) == 0);
    BOOST_CHECK(backend->fakeSet.size() == 0);
}

BOOST_AUTO_TEST_CASE(TwoChunk) {
    auto backend = std::make_shared<FakeBackend>();
    std::shared_ptr<ChunkResourceMgr> crm = ChunkResourceMgr::newMgr(backend);
    int scarray[] = {11, 12, 13, 14, 15};
    std::vector<int> subchunks(scarray, scarray+5);
    std::string thedb("Snowden");
    lsst::qserv::DbTableSet tables;
    tables.emplace(lsst::qserv::DbTable(thedb, "hello"));
    tables.emplace(lsst::qserv::DbTable(thedb, "goodbye"));
    {
        BOOST_CHECK(backend->fakeSet.size() == 0);
        BOOST_CHECK(crm->getRefCount(thedb, 1) == 0);
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 0);

        ChunkResource cr12345(crm->acquire(thedb, 12345, tables));
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 1);
        BOOST_CHECK(backend->fakeSet.size() == 0);

        ChunkResource cr12345sub(crm->acquire(thedb, 1, tables, subchunks));
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 1);
        BOOST_CHECK(crm->getRefCount(thedb, 1) == 1);
        BOOST_CHECK(backend->fakeSet.size() == 10); // 2 tables * 5 subchunks

        ChunkResource foo = cr12345sub;
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 1);
        BOOST_CHECK(crm->getRefCount(thedb, 1) == 2);
        BOOST_CHECK(backend->fakeSet.size() == 10);

        ChunkResource bar(cr12345sub);
        // now, these resources should be in acquired
        BOOST_CHECK(crm->getRefCount(thedb, 12345) == 1);
        BOOST_CHECK(crm->getRefCount(thedb, 1) == 3);
        BOOST_CHECK(backend->fakeSet.size() == 10);
    }
    // Now, these resources should be freed.
    BOOST_CHECK(crm->getRefCount(thedb, 12345) == 0);
    BOOST_CHECK(backend->fakeSet.size() == 0);
}

BOOST_AUTO_TEST_SUITE_END()
