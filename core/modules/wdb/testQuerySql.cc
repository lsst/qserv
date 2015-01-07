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
  /**
  * @brief Simple testing for class QuerySql
  *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "proto/worker.pb.h"
#include "wdb/QuerySql.h"
#include "wdb/QuerySql_Batch.h"

// Boost unit test header
#define BOOST_TEST_MODULE QuerySql_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using lsst::qserv::wdb::QuerySql;
using lsst::qserv::proto::TaskMsg_Subchunk;
using lsst::qserv::proto::TaskMsg_Fragment;

struct Fixture {

    Fixture() {
        defaultDb = "Winter";
        defaultResult = "myResult";
    }
    ~Fixture() {}

    TaskMsg_Fragment makeFragment() {
        TaskMsg_Fragment f;
        // "Real" subchunk query text should include
        // pre-substituted subchunk query text.
        f.add_query("SELECT o1.*, o2.* FROM Object_1001 o1, Object_1001 o2;");
        f.set_resulttable("fragResult");
        TaskMsg_Subchunk sc;
        sc.set_database(defaultDb);
        sc.add_table("Object");
        sc.add_id(1111);
        sc.add_id(1222);
        f.mutable_subchunks()->CopyFrom(sc);
        return f;
    }

    void printQsql(QuerySql const& q) {
        std::cout << "qsql=" << q << std::endl;
    }
    std::string defaultDb;
    std::string defaultResult;
};


BOOST_FIXTURE_TEST_SUITE(QuerySqlSuite, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {
    boost::shared_ptr<QuerySql>  qSql;
    TaskMsg_Fragment frag = makeFragment();
    qSql = boost::make_shared<QuerySql>(
                                        defaultDb,
                                        1001,
                                        frag,
                                        true,
                                        defaultResult
                                       );
    BOOST_CHECK(qSql.get());
    printQsql(*qSql);
}

BOOST_AUTO_TEST_CASE(QueryBatch) {
    boost::shared_ptr<QuerySql>  qSql;
    TaskMsg_Fragment frag = makeFragment();
    qSql = boost::make_shared<QuerySql>(
                                        defaultDb,
                                        1001,
                                        frag,
                                        true,
                                        defaultResult
                                       );
    BOOST_CHECK(qSql.get());

    QuerySql::Batch build("QueryBuildSub", qSql->buildList);
    QuerySql::Batch& batch=build;
    while(!batch.isDone()) {
        std::string piece = batch.current();
        batch.next();
    }
}


BOOST_AUTO_TEST_SUITE_END()
