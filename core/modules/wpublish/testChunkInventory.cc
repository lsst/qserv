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
/// Test ChunkInventory
#define BOOST_TEST_MODULE ChunkInventory_1
#include "boost/test/included/unit_test.hpp"
#include "wpublish/ChunkInventory.h"
#include "wlog/WLogger.h"
#include "sql/MockSql.h"

namespace test = boost::test_tools;
using lsst::qserv::wlog::WLogger;
using lsst::qserv::wpublish::ChunkInventory;
using lsst::qserv::sql::MockSql;
using lsst::qserv::sql::SqlResultIter;
using lsst::qserv::sql::SqlErrorObject;

namespace {
bool startswith(std::string const& a, std::string const& start) {
    return 0 == a.compare(0, start.length(), start);
}
}

struct ChunkInvFixture {
    ChunkInvFixture(void) {
    };
    ~ChunkInvFixture(void) {};

};

struct ChunkSql : public MockSql {
    ChunkSql() {
        Tuple t;
        t.push_back("LSST");
        _selectDbTuples.push_back(t);

    }
    virtual bool listTables(std::vector<std::string>& v,
                            SqlErrorObject& errObj,
                            std::string const& prefixed,
                            std::string const& dbName) {
        char const* tables[] = {"Object_31415", "Source_31415"};
        if(dbName == "LSST") {
            v.insert(v.begin(),
                     tables, tables + sizeof(tables)/sizeof(tables[0]));
            return true;
        } else {
            return false;
        }
    }
    virtual std::string getActiveDb() const {
        return std::string("LSST");
    }
    virtual boost::shared_ptr<SqlResultIter> getQueryIter(std::string const& query) {
        if(startswith(query, "SELECT db FROM")) {
            boost::shared_ptr<SqlIter> it;
            it.reset(new SqlIter(_selectDbTuples.begin(),
                                 _selectDbTuples.end()));
            return it;
        }
        return boost::shared_ptr<SqlIter>();
    }

    typedef std::vector<std::string> Tuple;
    typedef std::list<Tuple> TupleList;
    typedef TupleList::const_iterator TupleListIter;
    typedef MockSql::Iter<TupleListIter> SqlIter;

    TupleList _selectDbTuples;
    TupleList _nullTuples;
};

BOOST_FIXTURE_TEST_SUITE(ChunkInv, ChunkInvFixture)

BOOST_AUTO_TEST_CASE(Test1) {
    WLogger w;
    boost::shared_ptr<ChunkSql> cs(new ChunkSql());
    ChunkInventory ci("test", w, cs);
    BOOST_CHECK(ci.has("LSST", 31415));
    BOOST_CHECK(!ci.has("LSST", 123));
}

BOOST_AUTO_TEST_CASE(Test2) {
    WLogger w;
    boost::shared_ptr<ChunkSql> cs(new ChunkSql());
    ChunkInventory ci("test", w, cs);
    BOOST_CHECK(!ci.has("Winter2012", 31415));
    BOOST_CHECK(!ci.has("Winter2012", 123));
}
BOOST_AUTO_TEST_SUITE_END()
