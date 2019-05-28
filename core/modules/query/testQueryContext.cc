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

// System headers
#include <fstream>

// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/included/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <stdexcept>
#include <string>

// Qserv headers
#include "css/CssAccess.h"
#include "mysql/MySqlConfig.h"
#include "query/QueryContext.h"
#include "query/TableRef.h"
#include "query/TestFactory.h"

// Boost unit test header
#define BOOST_TEST_MODULE TableRef
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv::query;


struct TestFixture {
    TestFixture(void) : metaSession(0) {
        std::string kvMapPath = "./core/modules/qana/testPlugins.kvmap";
        std::ifstream stream(kvMapPath);
        css = lsst::qserv::css::CssAccess::createFromStream(stream, ".");
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
    // adding a table ref with the same alias (and other values) should fail.
    BOOST_REQUIRE_EQUAL(false, queryContext->addUsedTableRef(
        std::make_shared<TableRef>("db", "table", "alias")));
    // adding a table ref with the same alias (and different other values) should fail.
    BOOST_REQUIRE_EQUAL(false, queryContext->addUsedTableRef(
        std::make_shared<TableRef>("another_db", "another_table", "alias")));
    // adding a table ref with the same db & table but a different alias should pass
    auto tableRef2 = std::make_shared<TableRef>("db", "table", "another_alias");
    BOOST_REQUIRE_EQUAL(true, queryContext->addUsedTableRef(tableRef2));

    // getting a table ref with the same values as an added TableRef should return the originally enetered object.
    BOOST_REQUIRE_EQUAL(tableRef2.get(),
        queryContext->getTableRefMatch(std::make_shared<TableRef>("db", "table", "another_alias")).get()
    );

    // getting a table ref with a subset of the same values as an added TableRef should return the originally enetered object.
    BOOST_REQUIRE_EQUAL(tableRef2.get(),
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "table", "another_alias")).get()
    );
    BOOST_REQUIRE_EQUAL(tableRef2.get(),
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "", "another_alias")).get()
    );
    BOOST_REQUIRE_EQUAL(tableRef1.get(), // <- notice it gets the first object because same db & table name!
        queryContext->getTableRefMatch(std::make_shared<TableRef>("db", "table", "")).get()
    );
    BOOST_REQUIRE_EQUAL(tableRef1.get(),
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "table", "")).get()
    );

    // getting a table ref with the alias in the table position should return the originally enetered object.
    BOOST_REQUIRE_EQUAL(tableRef2.get(),
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "another_alias", "")).get()
    );

    // getting a table ref with non-matching values should return nullptr.
    BOOST_REQUIRE_EQUAL(nullptr,
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "", "some_other_alias")).get()
    );
    BOOST_REQUIRE_EQUAL(nullptr,
        queryContext->getTableRefMatch(std::make_shared<TableRef>("", "some_other_table", "")).get()
    );
    BOOST_REQUIRE_EQUAL(nullptr,
        queryContext->getTableRefMatch(std::make_shared<TableRef>("some_other_db", "some_other_table", "")).get()
    );
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
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",        "",       "", "column1")).get(),
                        columnRef1.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",   "table",       "", "column1")).get(),
                        columnRef1.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table",       "", "column1")).get(),
                        columnRef1.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",        "", "alias1", "column1")).get(),
                        columnRef1.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "alias1", "column1")).get(),
                        columnRef1.get());

    // try some expected-fail values
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "", "column3")).get(),
                        nullptr);
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias3", "column1")).get(),
                        nullptr);
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias2", "column1")).get(),
                        nullptr);
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "", "alias1", "column2")).get(),
                        nullptr);

    // get columnRef2 with a subset value match
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",        "",       "", "column2")).get(),
                        columnRef2.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",   "table",       "", "column2")).get(),
                        columnRef2.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table",       "", "column2")).get(),
                        columnRef2.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("",        "", "alias2", "column2")).get(),
                        columnRef2.get());
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("db", "table", "alias2", "column2")).get(),
                        columnRef2.get());

    // get columnRef1 with alias value match
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias1", "", "column1")).get(),
                        columnRef1.get());
    // get columnRef2 with alias value match
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias2", "", "column2")).get(),
                        columnRef2.get());
    // try some expected-fail alias matches
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias1", "", "column2")).get(),
                        nullptr);
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias2", "", "column1")).get(),
                        nullptr);
    BOOST_REQUIRE_EQUAL(queryContext->getValueExprMatch(ValueExpr::newColumnExpr("", "alias3", "", "column2")).get(),
                        nullptr);


}


BOOST_AUTO_TEST_SUITE_END()
