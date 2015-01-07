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
/// Test ChunkInventory

// Third-party headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "sql/MockSql.h"
#include "wpublish/ChunkInventory.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkInventory_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using lsst::qserv::sql::MockSql;
using lsst::qserv::sql::SqlResultIter;
using lsst::qserv::sql::SqlErrorObject;
using lsst::qserv::wpublish::ChunkInventory;

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
    ChunkSql(char const* const* tablesBegin, char const* const* tablesEnd)
        : _tablesBegin(tablesBegin), _tablesEnd(tablesEnd) {
        Tuple t;
        t.push_back("LSST");
        _selectDbTuples.push_back(t);

    }
    virtual bool listTables(std::vector<std::string>& v,
                            SqlErrorObject& errObj,
                            std::string const& prefixed,
                            std::string const& dbName) {
        if(dbName == "LSST") {
            v.insert(v.begin(), _tablesBegin, _tablesEnd);
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
            it = boost::make_shared<SqlIter>(
                                             _selectDbTuples.begin(),
                                             _selectDbTuples.end()
                                            );
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
    char const* const* _tablesBegin;
    char const* const* _tablesEnd;
};
char const* tables[] = {"Object_31415", "Source_31415",
                        "Object_1234567890", "Source_1234567890", };
int tablesSize = sizeof(tables)/sizeof(tables[0]);


BOOST_FIXTURE_TEST_SUITE(ChunkInv, ChunkInvFixture)

BOOST_AUTO_TEST_CASE(Test1) {
    boost::shared_ptr<ChunkSql> cs = boost::make_shared<ChunkSql>(tables, tables+tablesSize);
    ChunkInventory ci("test", cs);
    BOOST_CHECK(ci.has("LSST", 31415));
    BOOST_CHECK(ci.has("LSST", 1234567890));
    BOOST_CHECK(!ci.has("LSST", 123));
}

BOOST_AUTO_TEST_CASE(Test2) {
    boost::shared_ptr<ChunkSql> cs = boost::make_shared<ChunkSql>(tables, tables+tablesSize);
    ChunkInventory ci("test", cs);
    BOOST_CHECK(!ci.has("Winter2012", 31415));
    BOOST_CHECK(!ci.has("Winter2012", 123));
}
BOOST_AUTO_TEST_CASE(MissingDummy) {
    // Construct the mock without the dummy chunk
    boost::shared_ptr<ChunkSql> cs = boost::make_shared<ChunkSql>(tables, tables+2);
    // FIXME: enable when throwing on corrupt dbs is enabled.
    //BOOST_CHECK_THROW(new ChunkInventory("test", w, cs));
    ChunkInventory ci("test", cs);
    //ci.dbgPrint(std::cout);
    BOOST_CHECK(ci.has("LSST", 31415));
    BOOST_CHECK(!ci.has("LSST", 123));

}
BOOST_AUTO_TEST_SUITE_END()
