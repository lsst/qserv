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
    ChunkSql(std::vector<std::vector<std::string>> const& chunks,
             std::vector<std::vector<std::string>> const& workerId)

        :   _selectChunkTuples   (chunks),
            _selectWorkerIdTuples(workerId) {

        Tuple t;
        t.push_back("LSST");
        _selectDbTuples.push_back(t);        
    }
    virtual std::string getActiveDb() const {
        return std::string("LSST");
    }
    virtual std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query) {
        if (startswith(query, "SELECT db FROM"))
            return std::make_shared<SqlIter>(_selectDbTuples.begin(),
                                             _selectDbTuples.end  ());
        else if (startswith(query, "SELECT db,`table`,chunk FROM"))
            return std::make_shared<SqlIter>(_selectChunkTuples.begin(),
                                             _selectChunkTuples.end  ());
        else if (startswith(query, "SELECT id,created FROM"))
            return std::make_shared<SqlIter>(_selectWorkerIdTuples.begin(),
                                             _selectWorkerIdTuples.end  ());
        else
            return std::shared_ptr<SqlIter>();
    }

    typedef std::vector<std::string> Tuple;
    typedef std::vector<Tuple>       TupleVector;
    typedef TupleVector::const_iterator    TupleVectorIter;
    typedef MockSql::Iter<TupleVectorIter> SqlIter;

    TupleVector _selectDbTuples;
    TupleVector _selectChunkTuples;
    TupleVector _selectWorkerIdTuples;

};

std::vector<std::vector<std::string>> chunks = {
    {"LSST","Object_31415","31415"},
    {"LSST","Source_31415","31415"},
    {"LSST","Object_1234567890","1234567890"},
    {"LSST","Source_1234567890","1234567890"}
};
std::vector<std::vector<std::string>> chunksNoDummy = {
    {"LSST","Object_31415","31415"},
    {"LSST","Source_31415","31415"}
};
std::vector<std::vector<std::string>> workerId = {
    {"worker","2018-01-24 01:16:35"}
};

BOOST_FIXTURE_TEST_SUITE(ChunkInv, ChunkInvFixture)

BOOST_AUTO_TEST_CASE(Test1) {
    std::shared_ptr<ChunkSql> cs = std::make_shared<ChunkSql>(chunks, workerId);
    ChunkInventory ci("test", cs);
    BOOST_CHECK(ci.has("LSST", 31415));
    BOOST_CHECK(ci.has("LSST", 1234567890));
    BOOST_CHECK(!ci.has("LSST", 123));
}

BOOST_AUTO_TEST_CASE(Test2) {
    std::shared_ptr<ChunkSql> cs = std::make_shared<ChunkSql>(chunks, workerId);
    ChunkInventory ci("test", cs);
    BOOST_CHECK(!ci.has("Winter2012", 31415));
    BOOST_CHECK(!ci.has("Winter2012", 123));
}
BOOST_AUTO_TEST_CASE(MissingDummy) {
    // Construct the mock without the dummy chunk
    std::shared_ptr<ChunkSql> cs = std::make_shared<ChunkSql>(chunksNoDummy, workerId);
    // FIXME: enable when throwing on corrupt dbs is enabled.
    //BOOST_CHECK_THROW(new ChunkInventory("test", w, cs));
    ChunkInventory ci("test", cs);
    //ci.dbgPrint(std::cout);
    BOOST_CHECK(ci.has("LSST", 31415));
    BOOST_CHECK(!ci.has("LSST", 123));

}

BOOST_AUTO_TEST_CASE(WorkerId) {
    std::shared_ptr<ChunkSql> cs = std::make_shared<ChunkSql>(chunks, workerId);
    ChunkInventory ci("test", cs);
    BOOST_CHECK(ci.id() == "worker");
}

BOOST_AUTO_TEST_SUITE_END()
