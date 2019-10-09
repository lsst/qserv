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
#include "ccontrol/ParseRunner.h"
#include "ccontrol/ValidateQuery.h"
#include "query/SelectStmt.h"
#include "sql/Schema.h"
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE ValidateQuery
#include "boost/test/included/unit_test.hpp"

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)


class StatementAndSchema {
public:
    StatementAndSchema(
        std::string selectStmtSql_,
        sql::Schema schema_,
        std::string expectedErrorMessage_,
        bool shouldPass_) :
            selectStmtSql(selectStmtSql_),
            schema(schema_),
            expectedErrorMessage(expectedErrorMessage_),
            shouldPass(shouldPass_)
    {}

    std::string selectStmtSql;
    sql::Schema schema;
    std::string expectedErrorMessage;
    bool shouldPass;
};


std::ostream& operator<<(std::ostream& os, StatementAndSchema const& self) {
    os << "StatementAndSchema(";
    os << "select statement:" << self.selectStmtSql;
    os << ", schema:" << self.schema;
    os << ", expected error message:" << self.expectedErrorMessage;
    os << ")";
    return os;
}


static const std::vector<StatementAndSchema> STATEMENT_AND_SCHEMA = {
    // Verify the output when the error is not found:
    StatementAndSchema(
        "SELECT * FROM Object ORDER BY objectId",
        sql::Schema({sql::ColSchema("Object", "objectId", sql::ColType("bigint", 20))}),
        // right now no error causes the original error to get returned, with an error statement added.
        "", true),

    // Verify the ORDER BY reporting
    StatementAndSchema(
        "SELECT * FROM Object ORDER BY foo",
        sql::Schema({sql::ColSchema("Object", "objectId", sql::ColType("bigint", 20))}),
        // right now no error causes the original error to get returned.
        "Unknown column 'foo' in 'order clause'", false),

    // Verify the GROUP BY reporting
    StatementAndSchema(
        "SELECT * FROM Object GROUP BY foo",
        sql::Schema({sql::ColSchema("Object", "objectId", sql::ColType("bigint", 20))}),
        // right now no error causes the original error to get returned.
        "Unknown column 'foo' in 'group by clause'", false),

    // Verify   1. the HAVING reporting
    //          2. that a const val in the clause comes first and is allowed.
    StatementAndSchema(
        "SELECT * FROM Object GROUP BY objectId HAVING 20 < foo",
        sql::Schema({sql::ColSchema("Object", "objectId", sql::ColType("bigint", 20))}),
        // right now no error causes the original error to get returned.
        "Unknown column 'foo' in 'having clause'", false),
};


BOOST_DATA_TEST_CASE(testVerifyColumns, STATEMENT_AND_SCHEMA, testData) {
    std::string errorMessage;
    bool passed = ccontrol::validateQuery(ccontrol::ParseRunner::makeSelectStmt(testData.selectStmtSql),
                                          testData.schema,
                                          errorMessage);
    BOOST_CHECK_EQUAL(passed, testData.shouldPass);
    BOOST_CHECK_EQUAL(errorMessage, testData.expectedErrorMessage);
}

BOOST_AUTO_TEST_SUITE_END()
