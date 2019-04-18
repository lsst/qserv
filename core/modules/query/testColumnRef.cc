// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
  * @brief Simple testing for the ColumnRef class
  *
  */

// System headers
#include <fstream>

// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/included/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <string>

// Qserv headers
#include "query/ColumnRef.h"
#include "query/TableRef.h"

// Boost unit test header
#define BOOST_TEST_MODULE ColumnRef
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


namespace test = boost::test_tools;
using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)


std::shared_ptr<query::ColumnRef> makeColumnWithTable(std::string const& db, std::string const& table,
        std::string const& tableAlias, std::string const& column) {
    return std::make_shared<query::ColumnRef>(
        std::make_shared<query::TableRefBase>(db, table, tableAlias), column);
}


struct TestColumns {
    TestColumns(query::ColumnRef::Ptr CRa, query::ColumnRef::Ptr CRb, bool p)
    : a(CRa), b(CRb), pass(p)  {}

    TestColumns(std::string aDb, std::string aTable, std::string aColumn,
            std::string bDb, std::string bTable, std::string bColumn, bool p)
    : a(std::make_shared<query::ColumnRef>(aDb, aTable, aColumn))
    , b(std::make_shared<query::ColumnRef>(bDb, bTable, bColumn))
    , pass(p)
    {}

    TestColumns(std::string aDb, std::string aTable, std::string aTableAlias, std::string aColumn,
                std::string bDb, std::string bTable, std::string bTableAlias, std::string bColumn,
                bool p) : pass(p) {
        a = makeColumnWithTable(aDb, aTable, aTableAlias, aColumn);
        b = makeColumnWithTable(bDb, bTable, bTableAlias, bColumn);
    }

    friend std::ostream& operator<<(std::ostream& os, TestColumns const& self);

    query::ColumnRef::Ptr a;
    query::ColumnRef::Ptr b;
    bool pass; // if the test should pass; i.e. the available columns should satisfy the required columns.
};


// TestColumns plus an pass/fail indicator for using the alias
struct TestColumnsAlias : public TestColumns {
    TestColumnsAlias(std::string aDb, std::string aTable, std::string aTableAlias, std::string aColumn,
                     std::string bDb, std::string bTable, std::string bTableAlias, std::string bColumn,
                     bool noAliasPass, bool usingAliasPass)
    : TestColumns(aDb, aTable, aTableAlias, aColumn, bDb, bTable, bTableAlias, bColumn, noAliasPass)
    , aliasPass(usingAliasPass) {}

    bool aliasPass; // if the test should pass when using the TableRef alias (instead of the db & table vals);
};


std::ostream& operator<<(std::ostream& os, TestColumns const& self) {
    os << "TestColumns(";
    os << "a:" << self.a;
    os << ", b:" << self.b;
    os << ", expected match" << self.pass;
    os << ")";
    return os;
}

static const std::vector<TestColumns> COLUMN_REF_MATCHES = {

    TestColumns("",    "",    "foo",    "",    "",    "foo", true),  // match
    TestColumns("",    "",    "foo",    "",    "",    "bar", false), // mismatch: column
    TestColumns("",    "foo", "bar",    "",    "foo", "bar", true),  // match
    TestColumns("",    "foo", "bar",    "",    "foo", "foo", false), // mismatch: column
    TestColumns("",    "foo", "bar",    "",    "bar", "bar", false), // mismatch: table
    TestColumns("",    "foo", "bar",    "",    "bar", "foo", false), // mismatch: table, column
    TestColumns("foo", "bar", "baz",    "foo", "bar", "baz", true),  // match
    TestColumns("foo", "bar", "baz",    "foo", "bar", "bar", false), // mismatch: column
    TestColumns("foo", "bar", "baz",    "foo", "foo", "baz", false), // mismatch: table
    TestColumns("foo", "bar", "baz",    "bar", "bar", "baz", false), // mismatch: db
    TestColumns("foo", "",    "baz",    "foo", "bar", "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "bar", "baz",    "foo", "",    "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "bar", "",       "foo", "bar", "baz", false), // mismatch: column not populated
    TestColumns("foo", "bar", "baz",    "foo", "bar", "",    false), // mismatch: column not populated
    TestColumns("foo", "bar", "baz",    "",    "",    "baz", false), // mismatch: can't match db or table
    TestColumns("foo", "bar", "baz",    "",    "bar", "baz", false), // mismatch: can't match db
    TestColumns("",    "",    "baz",    "foo", "bar", "baz", true),  // match
    TestColumns("",    "baz", "foo",    "",    "",    "baz", false), // mismatch: don't know available table name
    TestColumns("",    "baz", "foo",    "",    "",    "foo", false), // mismatch: don't know available table name

    TestColumns("",    "",    "" , "baz",    "foo", "bar", "" , "baz", true),  // match
    TestColumns("",    "",    "a", "baz",    "foo", "bar", "b", "baz", false),  // mismatch: alias
    TestColumns("",    "",    "" , "foo",    "",    "",    "" , "bar", false), // mismatch: column
    TestColumns("",    "",    "a", "foo",    "",    "",    "b", "bar", false), // mismatch: column
    TestColumns("",    "",    "" , "foo",    "",    "",    "" , "foo", true),  // match
    TestColumns("",    "",    "a", "foo",    "",    "",    "b", "foo", false),  // mismatch: alias
    TestColumns("",    "baz", "" , "foo",    "",    "",    "" , "baz", false), // mismatch: don't know available table name
    TestColumns("",    "baz", "a", "foo",    "",    "",    "b", "baz", false), // mismatch: don't know available table name
    TestColumns("",    "baz", "" , "foo",    "",    "",    "" , "foo", false), // mismatch: don't know available table name
    TestColumns("",    "baz", "a", "foo",    "",    "",    "b", "foo", false), // mismatch: don't know available table name
    TestColumns("",    "foo", "" , "bar",    "",    "bar", "" , "bar", false), // mismatch: table
    TestColumns("",    "foo", "a", "bar",    "",    "bar", "b", "bar", false), // mismatch: table
    TestColumns("",    "foo", "" , "bar",    "",    "bar", "" , "foo", false), // mismatch: table, column
    TestColumns("",    "foo", "a", "bar",    "",    "bar", "b", "foo", false), // mismatch: table, column
    TestColumns("",    "foo", "" , "bar",    "",    "foo", "" , "bar", true),  // match
    TestColumns("",    "foo", "a", "bar",    "",    "foo", "b", "bar", false),  // mismatch: alias
    TestColumns("",    "foo", "" , "bar",    "",    "foo", "" , "foo", false), // mismatch: column
    TestColumns("",    "foo", "a", "bar",    "",    "foo", "b", "foo", false), // mismatch: column
    TestColumns("foo", "",    "" , "baz",    "foo", "bar", "" , "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "",    "a", "baz",    "foo", "bar", "b", "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "bar", "" , "",       "foo", "bar", "" , "baz", false), // mismatch: column not populated
    TestColumns("foo", "bar", "a", "",       "foo", "bar", "b", "baz", false), // mismatch: column not populated
    TestColumns("foo", "bar", "" , "baz",    "",    "",    "" , "baz", false), // mismatch: can't match db or table
    TestColumns("foo", "bar", "a", "baz",    "",    "",    "b", "baz", false), // mismatch: can't match db or table
    TestColumns("foo", "bar", "" , "baz",    "",    "bar", "" , "baz", false), // mismatch: can't match db
    TestColumns("foo", "bar", "a", "baz",    "",    "bar", "b", "baz", false), // mismatch: can't match db
    TestColumns("foo", "bar", "" , "baz",    "bar", "bar", "" , "baz", false), // mismatch: db
    TestColumns("foo", "bar", "a", "baz",    "bar", "bar", "b", "baz", false), // mismatch: db
    TestColumns("foo", "bar", "" , "baz",    "foo", "",    "" , "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "bar", "a", "baz",    "foo", "",    "b", "baz", false), // mismatch: db populated but table not
    TestColumns("foo", "bar", "" , "baz",    "foo", "bar", "" , "",    false), // mismatch: column not populated
    TestColumns("foo", "bar", "a", "baz",    "foo", "bar", "b", "",    false), // mismatch: column not populated
    TestColumns("foo", "bar", "" , "baz",    "foo", "bar", "" , "bar", false), // mismatch: column
    TestColumns("foo", "bar", "a", "baz",    "foo", "bar", "b", "bar", false), // mismatch: column
    TestColumns("foo", "bar", "" , "baz",    "foo", "bar", "" , "baz", true),  // match
    TestColumns("foo", "bar", "a", "baz",    "foo", "bar", "b", "baz", false),  // mismatch: alias
    TestColumns("foo", "bar", "" , "baz",    "foo", "foo", "" , "baz", false), // mismatch: table
    TestColumns("foo", "bar", "a", "baz",    "foo", "foo", "b", "baz", false), // mismatch: table
};


BOOST_DATA_TEST_CASE(ColumnRefMatches, COLUMN_REF_MATCHES, columns) {
    BOOST_REQUIRE_MESSAGE(columns.pass == columns.a->isSubsetOf(columns.b), columns.a <<
            (columns.pass ? "should " : "should NOT ") << "be a subset of " << columns.b);
}


static const std::vector<TestColumnsAlias> COLUMN_REF_LESS_THAN = {
    TestColumnsAlias("",   "",      "", "foo",      "",   "",      "", "foo", false, false),  // equal
    TestColumnsAlias("",   "table", "", "foo",      "",   "table", "", "foo", false, false),  // equal
    TestColumnsAlias("db", "table", "", "foo",      "db", "table", "", "foo", false, false),  // equal

    TestColumnsAlias("",   "",      "a", "foo",      "",   "",      "b", "foo", false, true),  // alias less than
    TestColumnsAlias("",   "table", "a", "foo",      "",   "table", "b", "foo", false, true),  //
    TestColumnsAlias("db", "table", "a", "foo",      "db", "table", "b", "foo", false, true),  //
    TestColumnsAlias("",   "",      "b", "foo",      "",   "",      "a", "foo", false, false), // alias greater than
    TestColumnsAlias("",   "table", "b", "foo",      "",   "table", "a", "foo", false, false), //
    TestColumnsAlias("db", "table", "b", "foo",      "db", "table", "a", "foo", false, false), //

    TestColumnsAlias("",   "a",     "b",  "c",      "",   "b",      "a", "a",   true, false),   // table less than, alias greater
    TestColumnsAlias("",   "b",     "a",  "a",      "",   "a",      "b",  "c",  false, true),   // table greater than, alias less

    TestColumnsAlias("a",   "e",     "b",  "c",      "b",   "e",      "a", "a",   true, false),   // db less than, alias greater
    TestColumnsAlias("b",   "e",     "a", "a",       "a",   "d",      "b",  "c",  false, true), // db greater than, alias less

};


BOOST_DATA_TEST_CASE(ColumnRefLessThan, COLUMN_REF_LESS_THAN, columns) {
    // check a < b
    BOOST_REQUIRE_MESSAGE(columns.pass == columns.a->lessThan(*columns.b, false),
            columns.a << " without alias " <<
            (columns.pass ? "should " : "should NOT ") << "be less than " << columns.b);
    BOOST_REQUIRE_MESSAGE(columns.aliasPass == columns.a->lessThan(*columns.b, true),
            columns.a << " with alias " <<
            (columns.aliasPass ? "should " : "should NOT ") << "be less than " << columns.b);
}





BOOST_AUTO_TEST_CASE(ColumnRefEqual) {
    // everything the same, check via alias
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "alais", "column").equal(
                        query::ColumnRef("db", "table", "alais", "column"), true), true);
    // everything the same, check via db & table
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "alais", "column").equal(
                        query::ColumnRef("db", "table", "alais", "column"), false), true);
    // different db and table, check via alias
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "alais", "column").equal(
                        query::ColumnRef("",   "",      "alais", "column"), true), true);
    // different db and table, check via db & table
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "alais", "column").equal(
                        query::ColumnRef("",   "",      "alais", "column"), false), false);
    // different alias, check via alias
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "a", "column").equal(
                        query::ColumnRef("db", "table", "alais", "column"), true), false);
    // different alias, check via db & table
    BOOST_REQUIRE_EQUAL(query::ColumnRef("db", "table", "a", "column").equal(
                        query::ColumnRef("db", "table", "alais", "column"), false), true);

}


BOOST_AUTO_TEST_SUITE_END()
