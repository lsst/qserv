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
 * @brief Simple testing for QueryPlugin implementations
 *
 */

// System headers
#include <fstream>
#include <list>
#include <vector>

// Third-party headers

// Qserv headers
#include "ccontrol/ParseRunner.h"
#include "ccontrol/UserQueryFactory.h"
#include "css/CssAccess.h"
#include "qana/TablePlugin.h"
#include "query/OrderByClause.h"
#include "query/SelectList.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/TestFactory.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "sql/SqlConfig.h"
#include "sql/SqlConnection.h"
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE TablePlugin
#include "boost/test/data/test_case.hpp"
#include <boost/test/unit_test.hpp>

using namespace lsst::qserv;
using lsst::qserv::query::TestFactory;

struct TestFixture {
    TestFixture(void)
            : schemaCfg(sql::SqlConfig(sql::SqlConfig::MockDbTableColumns(
                      {{"Somedb", {{"Object", {"objectId", "ra_PS", "decl_PS", "rFlux_PS_Sigma"}}}},
                       {"Very_Long_Database_Name_So_That_It_And_The",
                        {{"Table_are_65_char_long", {"ColumnName"}}}},
                       {"Long_Db_Name_So_That_It_And_The", {{"TandC_are_65_char_long", {"ColumnName"}}}}}))) {
        std::string kvMapPath = "./core/modules/qana/testPlugins.kvmap";  // (from testPlugins was: FIXME ??)
        std::ifstream stream(kvMapPath);
        css = lsst::qserv::css::CssAccess::createFromStream(stream, ".");
    }

    ~TestFixture(void) {}

    query::SelectStmt::Ptr makeStmtAndRunLogical(std::string query) {
        query::SelectStmt::Ptr selectStmt;
        BOOST_REQUIRE_NO_THROW(selectStmt = ccontrol::ParseRunner::makeSelectStmt(query));
        BOOST_REQUIRE(selectStmt != nullptr);
        query::TestFactory factory;
        std::shared_ptr<query::QueryContext> queryContext = factory.newContext(css, schemaCfg);
        auto&& tablePlugin = std::make_shared<qana::TablePlugin>();
        tablePlugin->applyLogical(*selectStmt, *queryContext);
        return selectStmt;
    }

    std::shared_ptr<lsst::qserv::css::CssAccess> css;
    sql::SqlConfig schemaCfg;
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

void REQUIRE_IS_COLUMN_REF(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs, int count) {
    if (count >= 0) {
        BOOST_REQUIRE_EQUAL(valueExprs.size(), size_t(count));
    }
    for (auto&& valueExpr : valueExprs) {
        BOOST_REQUIRE_EQUAL(valueExpr->isColumnRef(), true);
    }
}

struct TestData {
    TestData(std::string const& stmt_, std::string const& db_, std::string const& table_,
             std::string const& tableAlias_)
            : stmt(stmt_), expectedDb(db_), expectedTable(table_), expectedTableAlias(tableAlias_) {}

    std::string stmt;
    std::string expectedDb;
    std::string expectedTable;
    std::string expectedTableAlias;
};

std::ostream& operator<<(std::ostream& os, TestData const& testData) {
    os << "TestData(";
    os << "stmt:" << testData.stmt;
    os << "expected db:" << testData.expectedDb;
    os << "expected table:" << testData.expectedTable;
    os << "expected tableAlias:" << testData.expectedTableAlias;
    os << ")";
    return os;
}

static const std::vector<TestData> statements_1{
        TestData("SELECT        objectId FROM Object ORDER BY        objectId",
                 TestFactory::getDefaultDbName(), "Object", TestFactory::getDefaultDbName() + ".Object"),
        TestData("SELECT        objectId FROM Object ORDER BY Object.objectId",
                 TestFactory::getDefaultDbName(), "Object", TestFactory::getDefaultDbName() + ".Object"),
        TestData("SELECT Object.objectId FROM Object ORDER BY        objectId",
                 TestFactory::getDefaultDbName(), "Object", TestFactory::getDefaultDbName() + ".Object"),
        TestData("SELECT Object.objectId FROM Object ORDER BY Object.objectId",
                 TestFactory::getDefaultDbName(), "Object", TestFactory::getDefaultDbName() + ".Object"),

        TestData("SELECT o.objectId FROM Object o ORDER BY o.objectId", TestFactory::getDefaultDbName(),
                 "Object", "o"),
        TestData("SELECT   objectId FROM Object o ORDER BY o.objectId", TestFactory::getDefaultDbName(),
                 "Object", "o"),
        TestData("SELECT o.objectId FROM Object o ORDER BY   objectId", TestFactory::getDefaultDbName(),
                 "Object", "o"),
        TestData("SELECT   objectId FROM Object o ORDER BY   objectId", TestFactory::getDefaultDbName(),
                 "Object", "o"),

        TestData("SELECT Object.objectId FROM Object o ORDER BY      o.objectId",
                 TestFactory::getDefaultDbName(), "Object", "o"),
        TestData("SELECT        objectId FROM Object o ORDER BY Object.objectId",
                 TestFactory::getDefaultDbName(), "Object", "o"),

        TestData(
                "SELECT ColumnName FROM "
                "Very_Long_Database_Name_So_That_It_And_The.Table_are_65_char_long "  // 65 chars long; 1 more
                                                                                      // than the limit in
                                                                                      // TablePlugin::MYSQL_FIELD_MAX_LEN
                "ORDER BY ColumnName",
                "Very_Long_Database_Name_So_That_It_And_The", "Table_are_65_char_long", "tableRefAlias_0"),

};

// Test that the SelectStmt is rewritten by the TablePlugin so that the TableRef in the FROM list is the
// same as the one in the SELECT list, and that the ValueExpr in the SELECT list is the same as the one
// in the ORDER BY clause.
BOOST_DATA_TEST_CASE(PluginRewrite_1, statements_1, s) {
    auto&& selectStmt = makeStmtAndRunLogical(s.stmt);

    auto&& fromTableRefList = selectStmt->getFromList().getTableRefList();
    BOOST_REQUIRE_EQUAL(fromTableRefList.size(), size_t(1));
    BOOST_REQUIRE_EQUAL(fromTableRefList[0]->getDb(), s.expectedDb);
    BOOST_REQUIRE_EQUAL(fromTableRefList[0]->getTable(), s.expectedTable);
    BOOST_REQUIRE_EQUAL(fromTableRefList[0]->getAlias(), s.expectedTableAlias);

    // verify there is 1 value expr in the select list, and that it's a ColumnRef.
    auto&& selValExprList = *selectStmt->getSelectList().getValueExprList();
    REQUIRE_IS_COLUMN_REF(selValExprList, 1);

    // verify that the TableRef in the ColumnRef points same object as in the FromList.
    auto&& selColRef = selValExprList[0]->getFactor()->getColumnRef();
    auto&& fromTableRefs = selectStmt->getFromList().getTableRefList();
    BOOST_REQUIRE_EQUAL(fromTableRefs.size(), size_t(1));
    BOOST_REQUIRE_EQUAL(*selColRef->getTableRef(), *fromTableRefList[0]);

    // verify there is 1 value expr in the order by list, and that it is the same as the ValueExpr in the
    // select list.
    std::vector<std::shared_ptr<query::ValueExpr>> orderByValExprList;
    selectStmt->getOrderBy().findValueExprs(orderByValExprList);
    BOOST_REQUIRE_EQUAL(orderByValExprList.size(), size_t(1));
    BOOST_REQUIRE_EQUAL(*selValExprList[0], *orderByValExprList[0]);
}

BOOST_AUTO_TEST_CASE(PluginRewrite_2) {
    auto&& selectStmt = makeStmtAndRunLogical(
            "SELECT v.objectId, v.ra_PS, v.decl_PS "
            "FROM Object v, Object o "
            "WHERE o.objectId = 90030275138483 AND "
            "o.objectId != v.objectId AND "
            "scisql_angSep(v.ra_PS, v.decl_PS, o.ra_PS, o.decl_PS) < 0.016666 "
            "AND v.rFlux_PS_Sigma > 1e-32"
            "ORDER BY v.objectId");

    auto&& selValExprList = *selectStmt->getSelectList().getValueExprList();
    REQUIRE_IS_COLUMN_REF(selValExprList, 3);
    auto&& fromTableRefs = selectStmt->getFromList().getTableRefList();
    BOOST_REQUIRE_EQUAL(fromTableRefs.size(), size_t(2));
    // verify all 3 of the select val expr tables now point to the one from table 'v'.
    for (int i = 0; i < 3; ++i) {
        BOOST_REQUIRE_EQUAL(*selValExprList[i]->getColumnRef()->getTableRef(), *fromTableRefs[0]);
    }

    std::vector<std::shared_ptr<query::ValueExpr>> whereValExprList;
    selectStmt->getWhere()->findValueExprs(whereValExprList);
    BOOST_TEST_MESSAGE("WHERE:" << util::printable(whereValExprList));
    std::vector<std::shared_ptr<query::ValueExpr>> orderByValExprList;
    selectStmt->getOrderBy().findValueExprs(orderByValExprList);
    BOOST_TEST_MESSAGE("ORDER BY:" << util::printable(orderByValExprList));
}

BOOST_AUTO_TEST_CASE(LongValueExpr) {
    // Select list item is 65 chars long; 1 more than the limit in TablePlugin::MYSQL_FIELD_MAX_LEN
    std::string stmt =
            "SELECT Long_Db_Name_So_That_It_And_The.TandC_are_65_char_long.ColumnName "
            "FROM Long_Db_Name_So_That_It_And_The.TandC_are_65_char_long";
    auto selectStmt = makeStmtAndRunLogical(stmt);
    BOOST_REQUIRE_EQUAL((*selectStmt->getSelectList().getValueExprList())[0]->getAlias(), "valueExprAlias_0");
}

BOOST_AUTO_TEST_SUITE_END()
