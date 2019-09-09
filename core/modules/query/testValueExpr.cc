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
  /**
  *
  * @brief Simple testing for the TableRef class
  *
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
#include "query/FuncExpr.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/QueryTemplate.h"

// Boost unit test header
#define BOOST_TEST_MODULE ValueExpr
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv::query;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(subsetOf) {
    auto aliasInColumn = ValueExpr::newColumnExpr("alias");
    auto fullValue = ValueExpr::newColumnExpr("db", "table", "tableAlias", "column");
    fullValue->setAlias("alias");
    BOOST_REQUIRE_EQUAL(aliasInColumn->isSubsetOf(*fullValue), true);
}


BOOST_AUTO_TEST_CASE(renderValueExpr) {
    auto getRendered = [](std::shared_ptr<ValueExpr> const& valueExpr,
                          QueryTemplate::SetAliasMode aliasMode) -> std::string {
        QueryTemplate qt(aliasMode);
        ValueExpr::render render(qt, false);
        render.applyToQT(valueExpr);
        std::ostringstream os;
        os << qt;
        return os.str();
    };

    auto valueExpr = ValueExpr::newColumnExpr("db", "table", "tableAlias", "column");
    valueExpr->setAlias("alias");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "`tableAlias`.column AS `alias`");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`tableAlias`.column");
    BOOST_CHECK_THROW(getRendered(valueExpr, QueryTemplate::DEFINE_TABLE_ALIAS), std::runtime_error); // can't define table alias using a ValueExpr

    // no ValueExpr alias
    valueExpr = ValueExpr::newColumnExpr("db", "table", "tableAlias", "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "`tableAlias`.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "`tableAlias`.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`tableAlias`.column");

    // no TableRef alias
    valueExpr = ValueExpr::newColumnExpr("db", "table", "", "column"); // no table alias
    valueExpr->setAlias("alias");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "db.table.column AS `alias`");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "db.table.column");

    // no ValueExpr or TableRef alias
    valueExpr = ValueExpr::newColumnExpr("db", "table", "", "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "db.table.column");

    // no ValueExpr, TableRef alias, or database
    valueExpr = ValueExpr::newColumnExpr("", "table", "", "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "table.column");

    // no ValueExpr, TableRef alias, database, or table
    valueExpr = ValueExpr::newColumnExpr("column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_ALIAS), "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE_ALIAS), "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "column");
}


BOOST_AUTO_TEST_CASE(clone) {
    ValueExpr valueExpr;
    valueExpr.addValueFactor(ValueFactor::newAggFactor(FuncExpr::newArg1("MAX", "raFlux")));
    valueExpr.addOp(ValueExpr::MINUS);
    valueExpr.addValueFactor(ValueFactor::newAggFactor(FuncExpr::newArg1("MIN", "raFlux")));
    valueExpr.setAlias("flx");
    auto clonedValueExpr = valueExpr.clone();
    BOOST_CHECK_EQUAL(*clonedValueExpr, valueExpr);
    BOOST_CHECK(clonedValueExpr.get() != &valueExpr);
}



BOOST_AUTO_TEST_SUITE_END()
