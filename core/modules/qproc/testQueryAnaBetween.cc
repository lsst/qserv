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
#include <memory>
#include <string>
#include <vector>

// Third-party headers

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaBetween
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "tests/QueryAnaFixture.h"

using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::QsRestrictor;
using lsst::qserv::query::QueryContext;
using lsst::qserv::tests::QueryAnaFixture;

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(OrderBy, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(SecondaryIndex) {
    std::string stmt = "select * from Object where objectIdObjTest between 386942193651347 and 386942193651349;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndexBetween");
    char const* params[] = {"LSST", "Object", "objectIdObjTest", "386942193651347", "386942193651349"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+5);
}

BOOST_AUTO_TEST_SUITE_END()
