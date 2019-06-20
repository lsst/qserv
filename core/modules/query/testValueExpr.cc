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
#include "query/ValueExpr.h"
#include "query/QueryTemplate.h"

// Boost unit test header
#define BOOST_TEST_MODULE ValueExpr
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv::query;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(subsetOf) {
    auto aliasInColumn = ValueExpr::newColumnExpr("", "", "", "alias");
    auto fullValue = ValueExpr::newColumnExpr("db", "table", "tableAlias", "column");
    fullValue->setAlias("alias");
    BOOST_REQUIRE_EQUAL(aliasInColumn->isSubsetOf(*fullValue), true);
}


BOOST_AUTO_TEST_CASE(render) {
    auto valueExpr = ValueExpr::newColumnExpr("db", "table", "tableAlias", "column");
    valueExpr->setAlias("alias");

    auto getRendered = [](std::shared_ptr<ValueExpr> const& valueExpr,
                          QueryTemplate::AliasMode aliasMode,
                          QueryTemplate::AliasMode tableAliasMode) -> std::string {
        QueryTemplate qt;
        qt.setAliasMode(aliasMode);
        qt.setTableAliasMode(tableAliasMode);
        ValueExpr::render render(qt, false);
        render.applyToQT(valueExpr);
        std::ostringstream os;
        os << qt;
        return os.str();
    };

    // UseValueAlias
    // DefineValueUseTableAliases
    // DefineValueDontUseTableAliases
    // DefineTableAlias // illegal to render ValueExpr in this mode
    // NoValueUseTableAliases
    // NoAliases

    //                                       ValueExpr alias        TableRef alias
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::USE,    -),      "`alias`");

    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE, QueryTemplate::USE),      "`tableAlias`.column AS `alias`");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DEFINE, QueryTemplate::DONT_USE), "db.table.column AS `alias`");

    BOOST_CHECK_EQUAL(getRendered(valueExpr, -, QueryTemplate::DEFINE), "`tableAlias`");
    // DEFINE, DEFINE is nonsensical. WTD?

    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DONT_USE, QueryTemplate::DONT_USE), "db.table.column");
    BOOST_CHECK_EQUAL(getRendered(valueExpr, QueryTemplate::DONT_USE, QueryTemplate::USE), "`tableAlias`.column");

}



BOOST_AUTO_TEST_SUITE_END()
