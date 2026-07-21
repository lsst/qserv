// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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

// Qserv headers
#include "ccontrol/UserQueryType.h"

// Boost unit test header
#define BOOST_TEST_MODULE UserQueryType
#include <boost/test/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(testCallQueryType) {
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("CALL QSERV_RESULT_DELETE(1)"), true);
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("call QSERV_RESULT_DELETE(1,2,3)"), true);
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("submit call QSERV_RESULT_DELETE foo"), false);
}

BOOST_AUTO_TEST_CASE(testResultDelete) {
    std::string queryId;
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isResultDelete("CALL QSERV_RESULT_DELETE(1)", queryId), true);
    BOOST_CHECK_EQUAL(queryId, "1");
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isResultDelete("call qserv_result_delete( 42 ) ;", queryId),
                      true);
    BOOST_CHECK_EQUAL(queryId, "42");
    // Not QSERV_RESULT_DELETE: must not match (only supported CALL form).
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isResultDelete("CALL SOMETHING_ELSE(1)", queryId), false);
}

BOOST_AUTO_TEST_CASE(testSetQueryType) {
    std::string varName, varValue;
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isSetGlobal("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 0",
                                                           varName, varValue),
                      true);
    BOOST_CHECK_EQUAL(varName, "QSERV_ROW_COUNTER_OPTIMIZATION");
    BOOST_CHECK_EQUAL(varValue, "0");
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isSetGlobal("set global QSERV_ROW_COUNTER_OPTIMIZATION = 1;",
                                                           varName, varValue),
                      true);
    BOOST_CHECK_EQUAL(varValue, "1");

    // Non-integer values are accepted here; value validation is left to the caller.
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isSetGlobal("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = TRUE",
                                                           varName, varValue),
                      true);
    BOOST_CHECK_EQUAL(varValue, "TRUE");
    BOOST_CHECK_EQUAL(
            ccontrol::UserQueryType::isSetGlobal("SET GLOBAL SOME_VAR = 'a string'", varName, varValue),
            true);
    BOOST_CHECK_EQUAL(varName, "SOME_VAR");
    BOOST_CHECK_EQUAL(varValue, "'a string'");

    // An empty value is not a match
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isSetGlobal("SET GLOBAL SOME_VAR = ", varName, varValue),
                      false);

    // GLOBAL is required
    BOOST_CHECK_EQUAL(
            ccontrol::UserQueryType::isSetGlobal("SET QSERV_ROW_COUNTER_OPTIMIZATION = 0", varName, varValue),
            false);
}

BOOST_AUTO_TEST_SUITE_END()
