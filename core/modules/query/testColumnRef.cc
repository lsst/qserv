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

// Boost unit test header
#define BOOST_TEST_MODULE ColumnRef
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)




struct TestColumns {
    TestColumns(query::ColumnRef::Ptr CRa, query::ColumnRef::Ptr CRb, bool p)
    : a(CRa), b(CRb), pass(p)  {}

    TestColumns(std::string aDb, std::string aTable, std::string aColumn,
            std::string bDb, std::string bTable, std::string bColumn, bool p)
    : a(std::make_shared<query::ColumnRef>(aDb, aTable, aColumn))
    , b(std::make_shared<query::ColumnRef>(bDb, bTable, bColumn))
    , pass(p)
    {}

    friend std::ostream& operator<<(std::ostream& os, TestColumns const& self);

    query::ColumnRef::Ptr a;
    query::ColumnRef::Ptr b;
    bool pass; // if the test should pass; i.e. the available columns should satisfy the required columns.
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
};


BOOST_DATA_TEST_CASE(ColumnRefMatches, COLUMN_REF_MATCHES, columns) {
    BOOST_REQUIRE_MESSAGE(columns.pass == columns.a->matches(columns.b), "TODO");
}


BOOST_AUTO_TEST_SUITE_END()



