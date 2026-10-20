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
// (or that file could be included after boost/test/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <stdexcept>
#include <string>

// Qserv headers
#include "query/AndTerm.h"
#include "query/ColumnRef.h"
#include "query/FromList.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/QueryTemplate.h"
#include "query/TableRef.h"

// Boost unit test header
#define BOOST_TEST_MODULE TableRef
#include "boost/test/unit_test.hpp"
#include "boost/test/data/test_case.hpp"

using namespace lsst::qserv::query;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(verifyPopulated_noDb) {
    TableRef tableRef("", "table", "");
    BOOST_REQUIRE_THROW(tableRef.verifyPopulated(), std::logic_error);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_setDb) {
    TableRef tableRef("", "table", "");
    tableRef.verifyPopulated("database");
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_dbIsSet) {
    TableRef tableRef("database", "table", "");
    tableRef.verifyPopulated();
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_dbIsSetIgnoreDefault) {
    TableRef tableRef("database", "table", "");
    tableRef.verifyPopulated("otherDatabase");
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

struct TestTableRefs {
    TestTableRefs(std::shared_ptr<TableRef> const& TRa, std::shared_ptr<TableRef> const& TRb, bool p)
            : a(TRa), b(TRb), pass(p) {}

    TestTableRefs(std::string aDb, std::string aTable, std::string aAlias, std::string bDb,
                  std::string bTable, std::string bAlias, bool p)
            : a(std::make_shared<TableRef>(aDb, aTable, aAlias)),
              b(std::make_shared<TableRef>(bDb, bTable, bAlias)),
              pass(p) {}

    std::shared_ptr<TableRef> a;
    std::shared_ptr<TableRef> b;
    bool pass;  // if the test should pass;
};

std::ostream& operator<<(std::ostream& os, TestTableRefs const& self) {
    os << "TestTableRefs(";
    os << "a:" << self.a;
    os << ", b:" << self.b;
    os << ", expected match" << self.pass;
    os << ")";
    return os;
}

static const std::vector<TestTableRefs> TABLE_REF_SUBSET_TEST_DATA = {
        TestTableRefs("foo", "bar", "", "foo", "bar", "", true),   // match
        TestTableRefs("baz", "bar", "", "foo", "bar", "", false),  // database mismatch
        TestTableRefs("foo", "baz", "", "foo", "bar", "", false),  // table mismatch

        TestTableRefs("", "bar", "", "", "bar", "", true),     // match
        TestTableRefs("", "bar", "", "foo", "bar", "", true),  // match
        TestTableRefs("", "baz", "", "", "bar", "", false),    // table mismatch

        TestTableRefs("foo", "bar", "o", "foo", "bar", "o", true),  // match
        TestTableRefs("", "bar", "o", "foo", "bar", "o", true),     // match
        TestTableRefs("", "bar", "o", "", "bar", "s", false),       // alias mismatch
        TestTableRefs("", "baz", "o", "", "bar", "o",
                      false),  // column mismatch
                               // (but alias match! other consistency checks should catch this)

        TestTableRefs("", "Object", "", "database", "Object", "`database.Object`", true),  // match
};

BOOST_DATA_TEST_CASE(TableRefSubset, TABLE_REF_SUBSET_TEST_DATA, tables) {
    BOOST_REQUIRE_MESSAGE(
            tables.pass == tables.a->isSubsetOf(*tables.b),
            tables.a << (tables.pass ? "should " : "should NOT ") << "be a subset of " << tables.b);
}

static const std::vector<TestTableRefs> TABLE_REF_ALIASED_BY_TEST_DATA = {
        TestTableRefs("", "o", "", "database", "Object", "o", true),                 // match
        TestTableRefs("other_database", "o", "", "database", "Object", "o", false),  // mismatched database
        TestTableRefs("database", "Object", "o", "database", "Object", "o",
                      false),  // they match, but the former is not the latter in alias form
        TestTableRefs("", "Object", "", "database", "Object", "`database.Object`",
                      false),  // it is a subset match, not an alias match

};

BOOST_DATA_TEST_CASE(TableRefAliasedBy, TABLE_REF_ALIASED_BY_TEST_DATA, tables) {
    BOOST_REQUIRE_MESSAGE(
            tables.pass == tables.a->isAliasedBy(*tables.b),
            tables.a << (tables.pass ? "should " : "should NOT ") << "be aliased by " << tables.b);
}

BOOST_AUTO_TEST_CASE(renderTableRef) {
    auto getRendered = [](std::shared_ptr<TableRef> const& tableRef,
                          QueryTemplate::SetAliasMode aliasMode) -> std::string {
        QueryTemplate qt(aliasMode);
        TableRef::render render(qt);
        render.applyToQT(tableRef);
        std::ostringstream os;
        os << qt;
        return os.str();
    };

    auto tableRef = std::make_shared<TableRef>("db", "table", "alias");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_ALIAS), "`db`.`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::USE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_TABLE_ALIAS), "`db`.`table` AS `alias`");

    tableRef = std::make_shared<TableRef>("db", "table", "");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_ALIAS), "`db`.`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::USE_ALIAS), "`db`.`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS),
                      "`db`.`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`db`.`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_TABLE_ALIAS), "`db`.`table`");

    tableRef = std::make_shared<TableRef>("", "table", "alias");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_ALIAS), "`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::USE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`alias`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_TABLE_ALIAS), "`table` AS `alias`");

    tableRef = std::make_shared<TableRef>("", "table", "");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_ALIAS), "`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::USE_ALIAS), "`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS), "`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS), "`table`");
    BOOST_CHECK_EQUAL(getRendered(tableRef, QueryTemplate::DEFINE_TABLE_ALIAS), "`table`");
}

BOOST_AUTO_TEST_CASE(setDbWithoutTable) {
    TableRef tableRef;
    BOOST_REQUIRE_THROW(tableRef.setDb("db"), std::logic_error);
}

BOOST_AUTO_TEST_CASE(setEmptyTableWithDb) {
    TableRef tableRef("db", "table", "");
    BOOST_REQUIRE_THROW(tableRef.setTable(""), std::logic_error);
}

BOOST_AUTO_TEST_CASE(setEmptyTableInCtorWithDb) {
    BOOST_REQUIRE_THROW(TableRef tableRef("db", "", ""), std::logic_error);
}

BOOST_AUTO_TEST_CASE(renderFromListAfterJoin) {
    // Ensure table alias mode is set and restored appropriately
    for (auto const joinType : {JoinRef::DEFAULT, JoinRef::LEFT}) {
        auto joined = std::make_shared<TableRef>("db", "A", "a");
        auto joinRef =
                std::make_shared<JoinRef>(std::make_shared<TableRef>("db", "B", "b"), joinType, false,
                                          std::make_shared<JoinSpec>(std::make_shared<ColumnRef>("id")));
        joined->addJoin(joinRef);
        auto tables = std::make_shared<TableRefList>();
        tables->push_back(joined);
        tables->push_back(std::make_shared<TableRef>("db", "C", "c"));
        QueryTemplate qt(QueryTemplate::DEFINE_TABLE_ALIAS);
        FromList(tables).renderTo(qt);
        std::string const join = joinType == JoinRef::LEFT ? "LEFT OUTER JOIN" : "JOIN";
        BOOST_CHECK_EQUAL(qt.sqlFragment(),
                          "`db`.`A` AS `a` " + join + " `db`.`B` AS `b` USING(`id`),`db`.`C` AS `c`");
        BOOST_CHECK_EQUAL(qt.getAliasMode(), QueryTemplate::DEFINE_TABLE_ALIAS);

        QueryTemplate joinQt(QueryTemplate::NO_ALIAS);
        joinRef->putTemplate(joinQt);
        BOOST_CHECK_EQUAL(joinQt.sqlFragment(), join + " `db`.`B` AS `b` USING(`id`)");
        BOOST_CHECK_EQUAL(joinQt.getAliasMode(), QueryTemplate::NO_ALIAS);
    }
}

BOOST_AUTO_TEST_CASE(joinSpecValidation) {
    for (bool const hasOn : {false, true}) {
        for (bool const hasUsing : {false, true}) {
            BOOST_TEST_CONTEXT("ON=" << hasOn << ", USING=" << hasUsing) {
                JoinSpec spec(hasUsing ? std::make_shared<ColumnRef>("id") : nullptr,
                              hasOn ? std::make_shared<AndTerm>() : nullptr);
                QueryTemplate qt(QueryTemplate::DEFINE_TABLE_ALIAS);
                qt.append("existing");
                if (hasOn == hasUsing) {
                    BOOST_CHECK_THROW(spec.putTemplate(qt), std::logic_error);
                    BOOST_CHECK_THROW(spec.clone(), std::logic_error);
                    BOOST_CHECK_EQUAL(qt.sqlFragment(), "existing");
                } else {
                    BOOST_CHECK_NO_THROW(spec.putTemplate(qt));
                    auto cloned = spec.clone();
                    BOOST_REQUIRE(cloned != nullptr);
                    BOOST_CHECK(spec == *cloned);
                }
                BOOST_CHECK_EQUAL(qt.getAliasMode(), QueryTemplate::DEFINE_TABLE_ALIAS);
            }
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
