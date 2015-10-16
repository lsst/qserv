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

BOOST_AUTO_TEST_CASE(NoSecondaryIndex) {
    std::string stmt = "select * from Object where someField between 386942193651347 and 386942193651349;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(not context->restrictors);
}

BOOST_AUTO_TEST_CASE(DoubleSecondaryIndexRestrictor) {
    // FIXME: next query should be also supported:
    // std::string stmt = "select * from Object where objectIdObjTest between 38 and 40 OR objectIdObjTest IN (10, 30, 70);"
    // but this doesn't work: see DM-4017
    std::string stmt = "select * from Object where objectIdObjTest between 38 and 40 and objectIdObjTest IN (10, 30, 70);";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 2U);
    BOOST_REQUIRE(context->restrictors->at(0));
    QsRestrictor& restrictor0 = *context->restrictors->at(0);
    BOOST_CHECK_EQUAL(restrictor0._name, "sIndexBetween");
    char const* params0[] = {"LSST", "Object", "objectIdObjTest", "38", "40"};
    BOOST_CHECK_EQUAL_COLLECTIONS(restrictor0._params.begin(), restrictor0._params.end(),
                                  params0, params0+5);
    BOOST_REQUIRE(context->restrictors->at(1));
    QsRestrictor& restrictor1 = *context->restrictors->at(1);
    BOOST_CHECK_EQUAL(restrictor1._name, "sIndex");
    char const* params1[] = {"LSST", "Object", "objectIdObjTest", "10", "30", "70"};
    BOOST_CHECK_EQUAL_COLLECTIONS(restrictor1._params.begin(), restrictor1._params.end(),
                                  params1, params1+6);
}

BOOST_AUTO_TEST_CASE(DoubleSecondaryIndexRestrictorCartesian) {
    // This query has no astronomical meaning, but add additional test for cartesian product
    // FIXME: next query should be also supported:
    // std::string stmt = "select * from Object where objectIdObjTest between 38 and 40 OR objectIdObjTest IN (10, 30, 70);"
    // but this doesn't work: see DM-4017
    std::string stmt = "select * from Object o, Source s where o.objectIdObjTest between 38 and 40 AND s.objectIdSourceTest IN (10, 30, 70);";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 2U);
    BOOST_REQUIRE(context->restrictors->at(0));
    QsRestrictor& restrictor0 = *context->restrictors->at(0);
    BOOST_CHECK_EQUAL(restrictor0._name, "sIndexBetween");
    char const* params0[] = {"LSST", "Object", "objectIdObjTest", "38", "40"};
    BOOST_CHECK_EQUAL_COLLECTIONS(restrictor0._params.begin(), restrictor0._params.end(),
                                  params0, params0+5);
    BOOST_REQUIRE(context->restrictors->at(1));
    QsRestrictor& restrictor1 = *context->restrictors->at(1);
    BOOST_CHECK_EQUAL(restrictor1._name, "sIndex");
    char const* params1[] = {"LSST", "Object", "objectIdObjTest", "10", "30", "70"};
    BOOST_CHECK_EQUAL_COLLECTIONS(restrictor1._params.begin(), restrictor1._params.end(),
                                  params1, params1+6);
}



BOOST_AUTO_TEST_SUITE_END()
