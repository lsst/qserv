// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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
  * @file
  *
  * @brief Test functions and structures used in QueryAnalysis tests
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/SelectParser.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/QuerySession.h"
#include "query/Constraint.h"

// Boost unit test header
#include "boost/test/included/unit_test.hpp"

using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::Constraint;
using lsst::qserv::query::ConstraintVec;
using lsst::qserv::query::ConstraintVector;

namespace {

void testParse(SelectParser::Ptr p) {
    p->setup();
}

/**
* @brief Prepare the query session used to process SQL queries
* issued from MySQL client.
*
* @param t Test environment required by the object
* @param sql query to process
* @param expectedErr expected error message
* @param plugins plugin list loaded by the query session
* list will be loaded
*
*/
std::shared_ptr<QuerySession> buildQuerySession(QuerySession::Test& t,
                                                std::string const& stmt, std::string const& expectedErr = "") {
    std::shared_ptr<QuerySession> qs(new QuerySession(t));
    qs->setQuery(stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);

    // DEBUG step
    // FIXME handle log correctly
    if (LOG_CHECK_DEBUG() && expectedErr.empty()) {
        ConstraintVec cv(qs->getConstraints());
        std::shared_ptr<ConstraintVector> cvRaw = cv.getVector();
        if (cvRaw) {
            std::copy(cvRaw->begin(), cvRaw->end(),
                      std::ostream_iterator<Constraint>(std::cout, ","));
            typedef ConstraintVector::iterator Iter;
            for (Iter i = cvRaw->begin(), e = cvRaw->end(); i != e; ++i) {
                std::cout << *i << ",";
            }
        }
    }
    return qs;
}

std::string buildFirstParallelQuery(QuerySession& qs, bool withSubChunks=true) {
    qs.addChunk(ChunkSpec::makeFake(100, withSubChunks));
    QuerySession::Iter i = qs.cQueryBegin(), e = qs.cQueryEnd();
    BOOST_REQUIRE(i != e);
    ChunkQuerySpec& first = *i;
    return first.queries[0];
}

std::shared_ptr<QuerySession> check(QuerySession::Test& t,
                                    std::string const& stmt,
                                    std::string const& expectedParallel,
                                    std::string const& expectedErr = "",
                                    std::string const& expectedMerge = "") {

    bool testParallel = expectedErr.empty();
    bool testMerge = testParallel && !expectedMerge.empty();


    std::shared_ptr<QuerySession> qs = buildQuerySession(t, stmt, expectedErr);

    std::string sql;
    if(testParallel) {
        sql = buildFirstParallelQuery(*qs);
        BOOST_CHECK_EQUAL(sql, expectedParallel);
    }
    if (testMerge) {
        sql = qs->getMergeStmt()->toQueryTemplateString();
        BOOST_CHECK_EQUAL(sql, expectedMerge);
    }
    return qs;
}

void printChunkQuerySpecs(std::shared_ptr<QuerySession> qs) {
    for(QuerySession::Iter i = qs->cQueryBegin(),  e = qs->cQueryEnd();
        i != e; ++i) {
        lsst::qserv::qproc::ChunkQuerySpec& cs = *i;
        std::cout << "Spec: " << cs << std::endl;
    }
}

} // anonymous namespace

struct ParserFixture {
    ParserFixture(void) {
        qsTest.cfgNum = 0;
        qsTest.defaultDb = "LSST";
        // To learn how to dump the map, see qserv/core/css/KvInterfaceImplMem.cc
        // Use admin/examples/testMap_generateMap
        std::string mapBuffer(reinterpret_cast<char const*>(testMap),
                              testMap_length);
        std::istringstream mapStream(mapBuffer);
        std::string emptyChunkPath(".");
        qsTest.cssFacade =
            lsst::qserv::css::FacadeFactory::createMemFacade(mapStream,
                                                             emptyChunkPath);
    };
    ~ParserFixture(void) { };

    SelectParser::Ptr getParser(std::string const& stmt) {
        SelectParser::Ptr p = SelectParser::newInstance(stmt);
        p->setup();
        return p;
    }

    QuerySession::Test qsTest;
};
