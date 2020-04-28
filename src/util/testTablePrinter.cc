// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
  * @brief test TablePrinter
  */

// System headers
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Qserv headers
#include "util/TablePrinter.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE TablePrinter
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using namespace lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(TablePrinter) {

    LOGS_INFO("TablePrinter test begins");

    std::string const expectedResult1 =
" -- My Items --\n"
" -- ----+-------+---------+--------------------------------------------\n"
" --  id | name  |  weight | note                                      \n"
" -- ----+-------+---------+--------------------------------------------\n"
" --   0 | zero  |     2.1 | Lorem ipsum dolor sit amet                \n"
" --   1 | one   |    4.45 | consectetur adipiscing elit               \n"
" --   2 | two   |     222 | sed do eiusmod tempor incididunt ut labore\n"
" --   3 | three | 110.123 | et dolore magna aliqua.                   \n"
" --   4 | four  | -24.098 | Ut enim ad minim veniam...                \n"
" -- ----+-------+---------+--------------------------------------------\n";

    std::string const expectedResult2 =
" -- My Items --\n"
" -- \n"
" --  id | name  |  weight | note                                      \n"
" -- ----+-------+---------+--------------------------------------------\n"
" --   0 | zero  |     2.1 | Lorem ipsum dolor sit amet                \n"
" --   1 | one   |    4.45 | consectetur adipiscing elit               \n"
" --   2 | two   |     222 | sed do eiusmod tempor incididunt ut labore\n"
" --   3 | three | 110.123 | et dolore magna aliqua.                   \n"
" --   4 | four  | -24.098 | Ut enim ad minim veniam...                \n";

    std::string const expectedResult3 =
" -- My Items --\n"
" -- \n"
" --  id | name  |  weight | note                                      \n"
" -- ----+-------+---------+--------------------------------------------\n"
" --   0 | zero  |     2.1 | Lorem ipsum dolor sit amet                \n"
" --   1 | one   |    4.45 | consectetur adipiscing elit               \n"
" -- \n"
" --  id | name  |  weight | note                                      \n"
" -- ----+-------+---------+--------------------------------------------\n"
" --   2 | two   |     222 | sed do eiusmod tempor incididunt ut labore\n"
" --   3 | three | 110.123 | et dolore magna aliqua.                   \n"
" -- \n"
" --  id | name  |  weight | note                                      \n"
" -- ----+-------+---------+--------------------------------------------\n"
" --   4 | four  | -24.098 | Ut enim ad minim veniam...                \n";

    std::vector<int>         ids     = {0,      1,     2,     3,       4};
    std::vector<std::string> names   = {"zero", "one", "two", "three", "four"};
    std::vector<double>      weights = {2.1,    4.45,  222.,  110.123, -24.098};
    std::vector<std::string> notes   = {
      "Lorem ipsum dolor sit amet",
      "consectetur adipiscing elit",
      "sed do eiusmod tempor incididunt ut labore",
      "et dolore magna aliqua.",
      "Ut enim ad minim veniam..."
    };

    std::ostringstream result1;
    BOOST_REQUIRE_NO_THROW({
        ColumnTablePrinter table("My Items --", " -- ");
        table.addColumn("id",     ids);
        table.addColumn("name",   names, ColumnTablePrinter::Alignment::LEFT);
        table.addColumn("weight", weights);
        table.addColumn("note",   notes, ColumnTablePrinter::Alignment::LEFT);
        table.print(result1);
    });
    BOOST_CHECK(result1.str().size() == expectedResult1.size());
    BOOST_CHECK(result1.str() == expectedResult1);

    std::ostringstream result2;
    BOOST_REQUIRE_NO_THROW({
        ColumnTablePrinter table("My Items --", " -- ");
        table.addColumn("id",     ids);
        table.addColumn("name",   names, ColumnTablePrinter::Alignment::LEFT);
        table.addColumn("weight", weights);
        table.addColumn("note",   notes, ColumnTablePrinter::Alignment::LEFT);
        table.print(result2, false, false);
    });
    BOOST_CHECK(result2.str().size() == expectedResult2.size());
    BOOST_CHECK(result2.str() == expectedResult2);

    std::ostringstream result3;
    BOOST_REQUIRE_NO_THROW({
        ColumnTablePrinter table("My Items --", " -- ");
        table.addColumn("id",     ids);
        table.addColumn("name",   names, ColumnTablePrinter::Alignment::LEFT);
        table.addColumn("weight", weights);
        table.addColumn("note",   notes, ColumnTablePrinter::Alignment::LEFT);
        table.print(result3, false, false, 2, true);
    });
    BOOST_CHECK(result3.str().size() == expectedResult3.size());
    BOOST_CHECK(result3.str() == expectedResult3);

    // Test for expected exceptions

    std::vector<std::string> column        = {"00", "01", "02"};
    std::vector<std::string> shorterColumn = {"20", "21"};
    std::vector<std::string> longerColumn  = {"30", "31", "32", "33" };

    ColumnTablePrinter table("My Items --", " -- ");
    BOOST_REQUIRE_NO_THROW(table.addColumn("column",        column));
    BOOST_CHECK_THROW(     table.addColumn("shorterColumn", shorterColumn), std::invalid_argument);
    BOOST_CHECK_THROW(     table.addColumn("longerColumn",  longerColumn),  std::invalid_argument);

    LOGS_INFO("TablePrinter test ends");
}

BOOST_AUTO_TEST_SUITE_END()
