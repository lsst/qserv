// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @brief Test C++ parsing and query analysis logic for select expressions
  * with an "ORDER BY" clause.
  *
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <string>

// Third-party headers

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaAggregation
#include "boost/test/included/unit_test.hpp"

// LSST headers

// Qserv headers
#include "qproc/QuerySession.h"
#include "qproc/testQueryAna.h"
#include "query/QueryContext.h"

using lsst::qserv::query::SelectStmt;
using lsst::qserv::query::QueryContext;

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Aggregate, ParserFixture)

BOOST_AUTO_TEST_CASE(Aggregate) {
    std::string stmt = "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";
    std::string expPar = "SELECT sum(pm_declErr) AS QS1_SUM,chunkId,COUNT(bMagF2) AS QS2_COUNT,SUM(bMagF2) AS QS3_SUM FROM LSST.Object_100 AS QST_1_ WHERE bMagF>20.0 GROUP BY chunkId";

    std::shared_ptr<QuerySession> qs = buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();

    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_REQUIRE(ss.hasGroupBy());

    std::string parallel = buildFirstParallelQuery(*qs);
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_CASE(Avg) {
    std::string stmt = "select chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    std::string expPar = "SELECT chunkId,COUNT(bMagF2) AS QS1_COUNT,SUM(bMagF2) AS QS2_SUM FROM LSST.Object_100 AS QST_1_ WHERE bMagF>20.0";

    std::shared_ptr<QuerySession> qs = buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();

    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());

    std::string parallel = buildFirstParallelQuery(*qs);
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_SUITE_END()
