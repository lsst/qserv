// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
  * @file testFacade.cc
  *
  * @brief Unit test for the Facade class.
  *
  * @Author Jacek Becla, SLAC
  */


// standard library imports
#include <algorithm> // sort
#include <cstdlib>   // rand
#include <iostream>
#include <stdexcept>

// boost
#define BOOST_TEST_MODULE TestFacade
#include <boost/test/included/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

// local imports
#include "css/Facade.h"
#include "css/KvInterfaceImplZoo.h"
#include "css/CssException.h"

using std::cout;
using std::endl;
using std::make_pair;
using std::string;
using std::vector;

namespace lsst {
namespace qserv {
namespace css {
            

struct FacadeFixture {
    FacadeFixture(void) :
        prefix("/unittest_" + boost::lexical_cast<string>(rand())),
        facade(FacadeFactory::createZooTestFacade("localhost:2181", prefix)) {

        cout << "My prefix is: " << prefix << endl;
        kv.push_back(make_pair(prefix, ""));

        kv.push_back(make_pair(prefix + "/DATABASE_PARTITIONING", ""));
        string p = prefix + "/DATABASE_PARTITIONING/_0000000001";
        kv.push_back(make_pair(p, ""));
        kv.push_back(make_pair(p+"/nStripes", "18"));
        kv.push_back(make_pair(p+"/nSubStripes", "40"));
        kv.push_back(make_pair(p+"/overlap", "0.025"));

        kv.push_back(make_pair(prefix + "/DATABASES", ""));
        kv.push_back(make_pair(prefix + "/DATABASES/dbA", ""));
        kv.push_back(make_pair(prefix + "/DATABASES/dbA/partitioningId",
                               "0000000001"));
        kv.push_back(make_pair(prefix + "/DATABASES/dbB", ""));
        kv.push_back(make_pair(prefix + "/DATABASES/dbC", ""));
        p = prefix + "/DATABASES/dbA/TABLES";
        kv.push_back(make_pair(p, ""));
        kv.push_back(make_pair(p + "/Object", ""));
        kv.push_back(make_pair(p + "/Object/partitioning", ""));
        kv.push_back(make_pair(p + "/Object/partitioning/lonColName", "ra_PS"));
        kv.push_back(make_pair(p + "/Object/partitioning/latColName", "decl_PS"));
        kv.push_back(make_pair(p + "/Object/partitioning/subChunks", "1"));
        kv.push_back(make_pair(p + "/Object/partitioning/secIndexColName","objId"));
        kv.push_back(make_pair(p + "/Source", ""));
        kv.push_back(make_pair(p + "/Source/partitioning", ""));
        kv.push_back(make_pair(p + "/Source/partitioning/lonColName", "ra"));
        kv.push_back(make_pair(p + "/Source/partitioning/latColName", "decl"));
        kv.push_back(make_pair(p + "/Source/partitioning/subChunks", "0"));
        kv.push_back(make_pair(p + "/FSource", ""));
        kv.push_back(make_pair(p + "/FSource/partitioning", ""));
        kv.push_back(make_pair(p + "/FSource/partitioning/lonColName", "ra"));
        kv.push_back(make_pair(p + "/FSource/partitioning/latColName", "decl"));
        kv.push_back(make_pair(p + "/FSource/partitioning/subChunks", "0"));
        kv.push_back(make_pair(p + "/Exposure", ""));

        p = prefix + "/DATABASES/dbB/TABLES";
        kv.push_back(make_pair(p, ""));
        kv.push_back(make_pair(p + "/Exposure", ""));

        KvInterfaceImplZoo kvI = KvInterfaceImplZoo("localhost:2181");
        vector<std::pair<string, string> >::const_iterator itr;
        cout << "--------------" << endl;
        for (itr=kv.begin() ; itr!=kv.end() ; ++itr) {
            cout << itr->first << " --> " << itr->second << endl;
            kvI.create(itr->first, itr->second);
        }
        cout << "--------------" << endl;
    };

    ~FacadeFixture(void) {
        KvInterfaceImplZoo kvI = KvInterfaceImplZoo("localhost:2181");
        vector<std::pair<string, string> >::const_reverse_iterator itr;
        for (itr=kv.rbegin() ; itr!=kv.rend() ; ++itr) {
            kvI.deleteKey(itr->first);
        }
    };

    std::string prefix;
    vector<std::pair<string, string> > kv;
    boost::shared_ptr<Facade> facade;
};

BOOST_FIXTURE_TEST_SUITE(FacadeTest, FacadeFixture)

BOOST_AUTO_TEST_CASE(containsDb) {
    BOOST_CHECK_EQUAL(facade->containsDb("dbA"), true);
    BOOST_CHECK_EQUAL(facade->containsDb("dbB"), true);
    BOOST_CHECK_EQUAL(facade->containsDb("Dummy"), false);
}

BOOST_AUTO_TEST_CASE(containsTable) {
    // it does
    BOOST_CHECK_EQUAL(facade->containsTable("dbA", "Object"), true);

    // it does not
    BOOST_CHECK_EQUAL(facade->containsTable("dbA", "NotHere"), false);

    // for non-existing db
    BOOST_CHECK_THROW(facade->containsTable("Dummy", "NotHere"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(tableIsChunked) {
    // normal, table exists
    BOOST_CHECK_EQUAL(facade->tableIsChunked("dbA", "Object"), true);
    BOOST_CHECK_EQUAL(facade->tableIsChunked("dbA", "Source"), true);
    BOOST_CHECK_EQUAL(facade->tableIsChunked("dbA", "Exposure"), false);

    // normal, table does not exist
    BOOST_CHECK_THROW(facade->tableIsChunked("dbA", "NotHere"),
                      CssException_TableDoesNotExist);

    // for non-existing db
    BOOST_CHECK_THROW(facade->tableIsChunked("Dummy", "NotHere"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(tableIsSubChunked) {
    // normal, table exists
    BOOST_CHECK_EQUAL(facade->tableIsSubChunked("dbA", "Object"), true);
    BOOST_CHECK_EQUAL(facade->tableIsSubChunked("dbA", "Source"), false);
    BOOST_CHECK_EQUAL(facade->tableIsSubChunked("dbA", "Exposure"), false);

    // normal, table does not exist
    BOOST_CHECK_THROW(facade->tableIsSubChunked("dbA", "NotHere"),
                      CssException_TableDoesNotExist);

    // for non-existing db
    BOOST_CHECK_THROW(facade->tableIsSubChunked("Dummy", "NotHere"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(getAllowedDbs) {
    vector<string> v = facade->getAllowedDbs();
    BOOST_CHECK_EQUAL(3, v.size());
    std::sort (v.begin(), v.end());
    BOOST_CHECK_EQUAL(v[0], "dbA");
    BOOST_CHECK_EQUAL(v[1], "dbB");
    BOOST_CHECK_EQUAL(v[2], "dbC");
}

BOOST_AUTO_TEST_CASE(getChunkedTables) {
    // normal, 3 values
    vector<string> v = facade->getChunkedTables("dbA");
    BOOST_CHECK_EQUAL(3, v.size());
    std::sort (v.begin(), v.end());
    BOOST_CHECK_EQUAL(v[0], "FSource");
    BOOST_CHECK_EQUAL(v[1], "Object");
    BOOST_CHECK_EQUAL(v[2], "Source");

    // normal, no values
    v = facade->getChunkedTables("dbB");
    BOOST_CHECK_EQUAL(0, v.size());

    // for non-existing db
    BOOST_CHECK_THROW(facade->getChunkedTables("Dummy"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(getSubChunkedTables) {
    // normal, 2 values
    vector<string> v = facade->getSubChunkedTables("dbA");
    BOOST_CHECK_EQUAL(1, v.size());
    //std::sort (v.begin(), v.end());
    BOOST_CHECK_EQUAL(v[0], "Object");

    // normal, no values
    v = facade->getSubChunkedTables("dbB");
    BOOST_CHECK_EQUAL(0, v.size());

    // for non-existing db
    BOOST_CHECK_THROW(facade->getSubChunkedTables("Dummy"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(getPartitionCols) {
    // normal, has value
    vector<string> v = facade->getPartitionCols("dbA", "Object");
    BOOST_CHECK_EQUAL(v.size(), 3);
    BOOST_CHECK_EQUAL(v[0], "ra_PS");
    BOOST_CHECK_EQUAL(v[1], "decl_PS");
    BOOST_CHECK_EQUAL(v[2], "objId");

    v = facade->getPartitionCols("dbA", "Source");
    BOOST_CHECK_EQUAL(v.size(), 3);
    BOOST_CHECK_EQUAL(v[0], "ra");
    BOOST_CHECK_EQUAL(v[1], "decl");
    BOOST_CHECK_EQUAL(v[2], "");

    // for non-existing db
    BOOST_CHECK_THROW(facade->getPartitionCols("Dummy", "x"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(getChunkLevel) {
    BOOST_CHECK_EQUAL(facade->getChunkLevel("dbA", "Object"), 2);
    BOOST_CHECK_EQUAL(facade->getChunkLevel("dbA", "Source"), 1);
    BOOST_CHECK_EQUAL(facade->getChunkLevel("dbA", "Exposure"), 0);
}

BOOST_AUTO_TEST_CASE(getKeyColumn) {
    // normal, has value
    BOOST_CHECK_EQUAL(facade->getKeyColumn("dbA", "Object"), "objId");

    // normal, does not have value
    BOOST_CHECK_EQUAL(facade->getKeyColumn("dbA", "Source"), "");

    // for non-existing db
    BOOST_CHECK_THROW(facade->getKeyColumn("Dummy", "x"),
                      CssException_DbDoesNotExist);
}

BOOST_AUTO_TEST_CASE(getDbStriping) {
    StripingParams s = facade->getDbStriping("dbA");
    BOOST_CHECK_EQUAL(s.stripes, 18);
    BOOST_CHECK_EQUAL(s.subStripes, 40);
}

BOOST_AUTO_TEST_SUITE_END()

}}} // namespace lsst::qserv::css
