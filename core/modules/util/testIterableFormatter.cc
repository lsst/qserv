// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
 * @file
 *
 * @ingroup util
 *
 * @brief test IterableFormatter class
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers
#include <array>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

// Third-party headers

// Qserv headers
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE IterableFormatter
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;


BOOST_AUTO_TEST_SUITE(Suite)

/** @test
 * Print an empty vector
 */
BOOST_AUTO_TEST_CASE(EmptyVector) {

    test::output_test_stream output;
    std::vector<int> iterable {};
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[]"));
}

/** @test
 * Print a vector of int with default formatting
 */
BOOST_AUTO_TEST_CASE(Vector) {

    test::output_test_stream output;
    std::vector<int> iterable { 1, 2, 3, 4, 5, 6};
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a list of string with custom formatting
 */
BOOST_AUTO_TEST_CASE(Array) {
    test::output_test_stream output;
    std::array<std::string, 6> iterable { {"1", "2", "3", "4", "5", "6"} };
    auto start = std::next(iterable.begin(), 2);
    auto formatable = util::printable( start, iterable.end(), "", "", "; ");

    output << formatable;
    BOOST_REQUIRE(output.is_equal(R"("3"; "4"; "5"; "6")"));
}

class PrintableObj {
public:
    PrintableObj(int val) : _val(val) {}

    friend std::ostream& operator<<(std::ostream& os, PrintableObj const& self) {
        os << self._val;
        return os;
    }

private:
    int _val;
};


/** @test
 * Print a vector of objects
 */
BOOST_AUTO_TEST_CASE(Vector_of_object) {
    test::output_test_stream output;
    std::vector<PrintableObj> iterable { {PrintableObj(1), PrintableObj(2), PrintableObj(3),
        PrintableObj(4), PrintableObj(5), PrintableObj(6)} };
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a vector of objects owned by shared_ptr
 */
BOOST_AUTO_TEST_CASE(Vector_of_ptr_to_object) {
    test::output_test_stream output;
    std::vector<std::shared_ptr<PrintableObj>> iterable { {
        std::make_shared<PrintableObj>(1),
        std::make_shared<PrintableObj>(2),
        std::make_shared<PrintableObj>(3),
        std::make_shared<PrintableObj>(4),
        std::make_shared<PrintableObj>(5),
        std::make_shared<PrintableObj>(6)} };
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a vector of shared_ptr with a null ptr
 */
BOOST_AUTO_TEST_CASE(Vector_of_ptr_to_null_object) {
    test::output_test_stream output;
    std::vector<std::shared_ptr<PrintableObj>> iterable { {
        nullptr,
        std::make_shared<PrintableObj>(2),
        std::make_shared<PrintableObj>(3),
        std::make_shared<PrintableObj>(4),
        std::make_shared<PrintableObj>(5),
        std::make_shared<PrintableObj>(6)} };
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[nullptr, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a vector of objects owned by shared_ptr
 */
BOOST_AUTO_TEST_CASE(Pointer_to_vector_of_object) {
    test::output_test_stream output;
    auto iterable = std::make_shared<std::vector<PrintableObj>>();
    iterable->push_back(PrintableObj(1));
    iterable->push_back(PrintableObj(2));
    iterable->push_back(PrintableObj(3));
    iterable->push_back(PrintableObj(4));
    iterable->push_back(PrintableObj(5));
    iterable->push_back(PrintableObj(6));
    auto formatable = util::ptrPrintable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a pointer to a vector of pointer
 */
BOOST_AUTO_TEST_CASE(Ptr_to_vector_of_ptr_to_object) {
    test::output_test_stream output;
    auto iterable = std::make_shared<std::vector<std::shared_ptr<PrintableObj>>>();
    iterable->push_back(std::make_shared<PrintableObj>(1));
    iterable->push_back(std::make_shared<PrintableObj>(2));
    iterable->push_back(std::make_shared<PrintableObj>(3));
    iterable->push_back(std::make_shared<PrintableObj>(4));
    iterable->push_back(std::make_shared<PrintableObj>(5));
    iterable->push_back(std::make_shared<PrintableObj>(6));
    auto formatable = util::ptrPrintable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a map
 */
BOOST_AUTO_TEST_CASE(Map) {

    test::output_test_stream output;
    std::map<std::string, int> mapping{
        std::make_pair("a", 1),
        std::make_pair("b", 2),
        std::make_pair("x", 1001),
    };
    auto formatable = util::printable(mapping, "{", "}", "; ");

    output << formatable;
    BOOST_REQUIRE(output.is_equal(R"({("a", 1); ("b", 2); ("x", 1001)})"));
}

BOOST_AUTO_TEST_SUITE_END()
