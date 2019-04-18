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
  * @brief Simple testing for QueryPlugin implementations
  *
  */

// System headers
#include <fstream>
#include <list>
#include <vector>

// Qserv headers
#include "ccontrol/UserQueryFactory.h"
#include "css/CssAccess.h"
#include "mysql/MySqlConfig.h"
#include "parser/SelectParser.h"
#include "qana/TablePlugin.h"
#include "query/OrderByClause.h"
#include "query/SelectList.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/TestFactory.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE TablePlugin
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"

using namespace lsst::qserv;

struct TestFixture {
    TestFixture(void) {
        std::string kvMapPath = "./core/modules/qana/testPlugins.kvmap"; // (from testPlugins was: FIXME ??)
        std::ifstream stream(kvMapPath);
        css = lsst::qserv::css::CssAccess::createFromStream(stream, ".");
    }

    ~TestFixture(void) {}

    std::shared_ptr<lsst::qserv::css::CssAccess> css;
    lsst::qserv::mysql::MySqlConfig schemaCfg;
};


BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)


query::SelectStmt::Ptr makeStmtAndRunLogical(std::string query,
        std::shared_ptr<lsst::qserv::css::CssAccess> css, lsst::qserv::mysql::MySqlConfig schemaCfg) {
    query::SelectStmt::Ptr selectStmt;
    BOOST_REQUIRE_NO_THROW(selectStmt = parser::SelectParser::makeSelectStmt(query));
    BOOST_REQUIRE(selectStmt != nullptr);
    query::TestFactory factory;
    std::shared_ptr<query::QueryContext> queryContext = factory.newContext(css, schemaCfg);
    auto&& tablePlugin = std::make_shared<qana::TablePlugin>();
    tablePlugin->applyLogical(*selectStmt, *queryContext);
    return selectStmt;
}


void REQUIRE_IS_COLUMN_REF(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs, int count) {
    if (count >=0) {
        BOOST_REQUIRE_EQUAL(valueExprs.size(), size_t(count));
    }
    for (auto&& valueExpr : valueExprs) {
        BOOST_REQUIRE_EQUAL(valueExpr->isColumnRef(), true);
    }
}

std::vector<std::string> statements_1 {
    "SELECT        objectId FROM Object ORDER BY        objectId",
    "SELECT        objectId FROM Object ORDER BY Object.objectId",
    "SELECT Object.objectId FROM Object ORDER BY        objectId",
    "SELECT Object.objectId FROM Object ORDER BY Object.objectId",

    "SELECT o.objectId FROM Object o ORDER BY o.objectId",
    "SELECT   objectId FROM Object o ORDER BY o.objectId",
    "SELECT o.objectId FROM Object o ORDER BY   objectId",
    "SELECT   objectId FROM Object o ORDER BY   objectId",

    "SELECT Object.objectId FROM Object o ORDER BY      o.objectId",
    "SELECT        objectId FROM Object o ORDER BY Object.objectId",

};


// Test that the SelectStmt is rewritten by the TablePlugin so that the TableRef in the FROM list is the
// same as the one in the SELECT list, and that the ValueExpr in the SELECT list is the same as the one
// in the ORDER BY clause.
BOOST_DATA_TEST_CASE(PluginRewrite_1, statements_1, statement) {
    auto&& selectStmt = makeStmtAndRunLogical(statement, css, schemaCfg);

    // verify there is 1 value expr in the select list, and that it's a ColumnRef.
    auto&& selValExprList = *selectStmt->getSelectList().getValueExprList();
    REQUIRE_IS_COLUMN_REF(selValExprList, 1);

    // verify that the TableRef in the ColumnRef is the same object as in the FromList.
    auto&& selColRef = selValExprList[0]->getFactor()->getColumnRef();
    auto&& fromTableRefs = selectStmt->getFromList().getTableRefList();
    BOOST_REQUIRE_EQUAL(fromTableRefs.size(), size_t(1));
    BOOST_REQUIRE_EQUAL(selColRef->getTableRef(), fromTableRefs[0]);

    // verify there is 1 value expr in the order by list, and that it is the same as the ValueExpr in the select list.
    std::vector<std::shared_ptr<query::ValueExpr>> orderByValExprList;
    selectStmt->getOrderBy().findValueExprs(orderByValExprList);
    BOOST_REQUIRE_EQUAL(orderByValExprList.size(), size_t(1));
    // below is a pointer compare, not value compare; verify they point at the same object.
    BOOST_REQUIRE_EQUAL(selValExprList[0].get(), orderByValExprList[0].get());
}


BOOST_AUTO_TEST_CASE(PluginRewrite_2) {
    auto&& selectStmt = makeStmtAndRunLogical(
        "SELECT v.objectId, v.ra_PS, v.decl_PS "
        "FROM Object v, Object o "
        "WHERE o.objectId = 90030275138483 AND "
               "o.objectId != v.objectId AND "
               "scisql_angSep(v.ra_PS, v.decl_PS, o.ra_PS, o.decl_PS) < 0.016666 "
               "AND v.rFlux_PS_Sigma > 1e-32"
        "ORDER BY v.objectId",
        css, schemaCfg);

    auto&& selValExprList = *selectStmt->getSelectList().getValueExprList();
    REQUIRE_IS_COLUMN_REF(selValExprList, 3);
    auto&& fromTableRefs = selectStmt->getFromList().getTableRefList();
    BOOST_REQUIRE_EQUAL(fromTableRefs.size(), size_t(2));
    // verify all 3 of the select val expr tables now point to the one from table 'v'.
    for (int i = 0; i<3; ++i) {
        BOOST_REQUIRE_EQUAL(selValExprList[i]->getColumnRef()->getTableRef(), fromTableRefs[0]);
    }

    std::vector<std::shared_ptr<query::ValueExpr>> whereValExprList;
    selectStmt->getWhere()->findValueExprs(whereValExprList);
    BOOST_TEST_MESSAGE("WHERE:" << util::printable(whereValExprList));
    std::vector<std::shared_ptr<query::ValueExpr>> orderByValExprList;
    selectStmt->getOrderBy().findValueExprs(orderByValExprList);
    BOOST_TEST_MESSAGE("ORDER BY:" << util::printable(orderByValExprList));
}


BOOST_AUTO_TEST_SUITE_END()
