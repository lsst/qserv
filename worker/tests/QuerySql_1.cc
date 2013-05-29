/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file QuerySql_1.cc
  *
  * @brief Simple testing for class QuerySql
  *
  * @author Daniel L. Wang, SLAC
  */ 
#define BOOST_TEST_MODULE QuerySql_1
#include "boost/test/included/unit_test.hpp"
#include "lsst/qserv/worker/QuerySql.h"
#include "lsst/qserv/worker/QuerySql_Batch.h"

namespace test = boost::test_tools;
namespace qWorker = lsst::qserv::worker;

using lsst::qserv::worker::QuerySql;
using lsst::qserv::TaskMsg_Subchunk;

struct Fixture {
    typedef qWorker::Task Task;

    Fixture() {
        defaultDb = "Winter"; 
        defaultResult = "myResult";
    }
    ~Fixture() {}

    Task::Fragment makeFragment() {
        Task::Fragment f;
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
    QuerySql::Factory factory;
    std::string defaultDb;
    std::string defaultResult;
};


BOOST_FIXTURE_TEST_SUITE(QuerySqlSuite, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {
    boost::shared_ptr<QuerySql>  qSql;
    Task::Fragment frag = makeFragment();
    qSql = factory.make(defaultDb, 1001, frag, true, defaultResult);
    BOOST_CHECK(qSql.get());    
    printQsql(*qSql);
}

BOOST_AUTO_TEST_CASE(QueryBatch) {
    boost::shared_ptr<QuerySql>  qSql;
    Task::Fragment frag = makeFragment();
    qSql = factory.make(defaultDb, 1001, frag, true, defaultResult);
    BOOST_CHECK(qSql.get());    

    QuerySql::Batch build("QueryBuildSub", qSql->buildList);
    QuerySql::Batch& batch=build;
    int i=0;
    while(!batch.isDone()) {
        std::string piece = batch.current();
        batch.next();
    }
}


BOOST_AUTO_TEST_SUITE_END()
