// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

// System headers
#include <fstream>

// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <stdexcept>
#include <string>

// Qserv headers
#include "css/CssAccess.h"
#include "mysql/MySqlConfig.h"
#include "query/QueryContext.h"
#include "query/JoinRef.h"
#include "query/TableRef.h"
#include "query/TestFactory.h"

// Boost unit test header
#define BOOST_TEST_MODULE TableRef
#include "boost/test/unit_test.hpp"

using namespace lsst::qserv::query;

const std::string testPlugins(R"BEGINEND({
    "\/css_meta": "",
    "\/css_meta\/version": "1",
    "\/DBS": "",
    "\/DBS\/LSST": "READY",
    "\/DBS\/LSST\/LOCK": "",
    "\/DBS\/LSST\/LOCK\/comments": "",
    "\/DBS\/LSST\/LOCK\/estimatedDuration": "",
    "\/DBS\/LSST\/LOCK\/lockedBy": "",
    "\/DBS\/LSST\/LOCK\/lockedTime": "",
    "\/DBS\/LSST\/LOCK\/mode": "",
    "\/DBS\/LSST\/LOCK\/reason": "",
    "\/DBS\/LSST\/partitioningId": "0000000000",
    "\/DBS\/LSST\/releaseStatus": "UNRELEASED",
    "\/DBS\/LSST\/storageClass": "L2",
    "\/DBS\/LSST\/TABLES": "",
    "\/DBS\/LSST\/TABLES\/Object": "READY",
    "\/DBS\/LSST\/TABLES\/Object\/compression": "0",
    "\/DBS\/LSST\/TABLES\/Object\/match": "0",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning": "",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning\/dirColName": "objectIdObjTest",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning\/dirTable": "Object",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning\/latColName": "decl_Test",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning\/lonColName": "ra_Test",
    "\/DBS\/LSST\/TABLES\/Object\/partitioning\/subChunks": "1",
    "\/DBS\/LSST\/TABLES\/Source": "READY",
    "\/DBS\/LSST\/TABLES\/Source\/compression": "0",
    "\/DBS\/LSST\/TABLES\/Source\/match": "0",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning": "",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning\/dirColName": "objectIdSourceTest",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning\/dirTable": "Object",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning\/latColName": "declObjectTest",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning\/lonColName": "raObjectTest",
    "\/DBS\/LSST\/TABLES\/Source\/partitioning\/subChunks": "0",
    "\/DBS\/Somedb": "READY",
    "\/DBS\/Somedb\/LOCK": "",
    "\/DBS\/Somedb\/LOCK\/comments": "",
    "\/DBS\/Somedb\/LOCK\/estimatedDuration": "",
    "\/DBS\/Somedb\/LOCK\/lockedBy": "",
    "\/DBS\/Somedb\/LOCK\/lockedTime": "",
    "\/DBS\/Somedb\/LOCK\/mode": "",
    "\/DBS\/Somedb\/LOCK\/reason": "",
    "\/DBS\/Somedb\/partitioningId": "0000000001",
    "\/DBS\/Somedb\/releaseStatus": "UNRELEASED",
    "\/DBS\/Somedb\/storageClass": "L2",
    "\/DBS\/Somedb\/TABLES": "",
    "\/DBS\/Somedb\/TABLES\/Bar": "READY",
    "\/PARTITIONING": "",
    "\/PARTITIONING\/_0000000000": "",
    "\/PARTITIONING\/_0000000000\/nStripes": "60",
    "\/PARTITIONING\/_0000000000\/nSubStripes": "18",
    "\/PARTITIONING\/_0000000000\/overlap": "0.025"
})BEGINEND");

struct TestFixture {
    TestFixture(void) : metaSession(0) {
        css = lsst::qserv::css::CssAccess::createFromData(testPlugins, ".");
    }

    ~TestFixture(void) {}

    std::shared_ptr<lsst::qserv::css::CssAccess> css;
    lsst::qserv::mysql::MySqlConfig schemaCfg;
    int metaSession;
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(UsedTables) {
    TestFactory factory;
    auto queryContext = factory.newContext(css, schemaCfg);
    // add a table ref
    auto tableRef1 = std::make_shared<TableRef>("db", "table", "alias");
    BOOST_REQUIRE_EQUAL(true, queryContext->addUsedTableRef(tableRef1));
    // adding a table ref with the same values should pass.
    BOOST_REQUIRE_EQUAL(true,
                        queryContext->addUsedTableRef(std::make_shared<TableRef>("db", "table", "alias")));
    // adding a table ref with the same alias (and different other values) should fail.
    BOOST_REQUIRE_EQUAL(false, queryContext->addUsedTableRef(
                                       std::make_shared<TableRef>("another_db", "another_table", "alias")));
    // adding a table ref with the same db & table but a different alias should pass
    auto tableRef2 = std::make_shared<TableRef>("db", "table", "another_alias");
    BOOST_REQUIRE_EQUAL(true, queryContext->addUsedTableRef(tableRef2));

    // getting a table ref with the same values as an added TableRef should return the originally enetered
    // object.
    auto tableRefMatch =
            queryContext->getTableRefMatch(std::make_shared<TableRef>("db", "table", "another_alias"));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef2, *tableRefMatch);

    // getting a table ref with a subset of the same values as an added TableRef should return the originally
    // enetered object.
    tableRefMatch = queryContext->getTableRefMatch(std::make_shared<TableRef>("", "table", "another_alias"));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef2, *tableRefMatch);
    tableRefMatch = queryContext->getTableRefMatch(std::make_shared<TableRef>("", "", "another_alias"));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef2, *tableRefMatch);
    tableRefMatch = queryContext->getTableRefMatch(std::make_shared<TableRef>("db", "table", ""));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef1,
                        *tableRefMatch);  // <- notice it gets the first object because same db & table name!
    tableRefMatch = queryContext->getTableRefMatch(std::make_shared<TableRef>("", "table", ""));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef1.get(), *tableRefMatch);

    // getting a table ref with the alias in the table position should return the originally enetered object.
    tableRefMatch = queryContext->getTableRefMatch(std::make_shared<TableRef>("", "another_alias", ""));
    BOOST_REQUIRE(tableRefMatch != nullptr);
    BOOST_REQUIRE_EQUAL(*tableRef2, *tableRefMatch);

    // getting a table ref with non-matching values should return nullptr.
    BOOST_REQUIRE_EQUAL(
            nullptr, queryContext->getTableRefMatch(std::make_shared<TableRef>("", "", "some_other_alias")));
    BOOST_REQUIRE_EQUAL(
            nullptr, queryContext->getTableRefMatch(std::make_shared<TableRef>("", "some_other_table", "")));
    BOOST_REQUIRE_EQUAL(nullptr, queryContext->getTableRefMatch(std::make_shared<TableRef>(
                                         "some_other_db", "some_other_table", "")));
}

BOOST_AUTO_TEST_CASE(UsedSelectListValueExprs) {
    TestFactory factory;
    auto queryContext = factory.newContext(css, schemaCfg);
    // add a ColumnRef ValueExpr
    auto columnRef1 = ValueExpr::newColumnExpr("db", "table", "alias1", "column1");
    queryContext->addUsedValueExpr(columnRef1);
    // add a different ColumnRef ValueExpr
    auto columnRef2 = ValueExpr::newColumnExpr("db", "table", "alias2", "column2");
    queryContext->addUsedValueExpr(columnRef2);

    // get columnRef1 with a subset value match
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "", "column1")).get(),
            columnRef1.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "table", "", "column1")).get(),
            columnRef1.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "", "column1")).get(),
            columnRef1.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias1", "column1")).get(),
            columnRef1.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "alias1", "column1"))
                    .get(),
            columnRef1.get());

    // try some expected-fail values
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "", "column3")).get(), nullptr);
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias3", "column1")).get(),
            nullptr);
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias2", "column1")).get(),
            nullptr);
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias1", "column2")).get(),
            nullptr);

    // get columnRef2 with a subset value match
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "", "column2")).get(),
            columnRef2.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "table", "", "column2")).get(),
            columnRef2.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "", "column2")).get(),
            columnRef2.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias2", "column2")).get(),
            columnRef2.get());
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "alias2", "column2"))
                    .get(),
            columnRef2.get());

    // get columnRef1 with alias value match
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias1", "", "column1")).get(),
            columnRef1.get());
    // get columnRef2 with alias value match
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias2", "", "column2")).get(),
            columnRef2.get());
    // try some expected-fail alias matches
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias1", "", "column2")).get(),
            nullptr);
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias2", "", "column1")).get(),
            nullptr);
    BOOST_REQUIRE_EQUAL(
            queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias3", "", "column2")).get(),
            nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
