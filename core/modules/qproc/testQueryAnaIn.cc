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
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/format.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaIn
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qproc/testQueryAna.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"

using lsst::qserv::query::QsRestrictor;
using lsst::qserv::query::QueryContext;

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(OrderBy, ParserFixture)

BOOST_AUTO_TEST_CASE(SecondaryIndex) {
    std::string stmt = "select * from Object where objectIdObjTest in (2,3145,9999);";
    std::shared_ptr<QuerySession> qs = buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);
}
BOOST_AUTO_TEST_CASE(CountIn) {
    std::string stmt = "select COUNT(*) AS N FROM Source WHERE objectId IN(386950783579546, 386942193651348);";
    std::shared_ptr<QuerySession> qs = buildQuerySession(qsTest, stmt);
    std::string expectedParallel = "SELECT COUNT(*) AS QS1_COUNT FROM LSST.Source_100 AS QST_1_ "
                                   "WHERE objectId IN(386950783579546,386942193651348)";
    std::string expectedMerge = "SELECT SUM(QS1_COUNT) AS N";
    auto querySession = check(qsTest, stmt, expectedParallel, "", expectedMerge);
    for(QuerySession::Iter i = querySession->cQueryBegin(),  e = querySession->cQueryEnd();
        i != e; ++i) {
        lsst::qserv::qproc::ChunkQuerySpec& cs = *i;
        LOGF_INFO("Chunk spec: %1%" % cs );
    }
    querySession->cQueryBegin();
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(context->hasChunks());
}

BOOST_AUTO_TEST_SUITE_END()
