/*
 * LSST Data Management System
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
  * @brief test ReplicaInfo
  */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>
#include <sstream>
#include <vector>

// Qserv headers
#include "replica/Csv.h"

// Boost unit test header
#define BOOST_TEST_MODULE JsonLibrary
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(TestCsv) {

    LOGS_INFO("TestCsv test begins");

    csv::Dialect const dialect;
    csv::Parser parser(dialect);
    string const in = "Line 1\\\nLine 2\nNon-terminated line 3";
    bool const flush = true;
    vector<string> lines;
    parser.parse(in.data(), in.size(), flush, [&lines](char const* out, size_t size) {
        lines.emplace_back(string(out, size));
    });
    for (auto const& line: lines) {
        LOGS_INFO("TestCsv: " + line);
    }
    LOGS_INFO("TestCsv test ends");
}


BOOST_AUTO_TEST_SUITE_END()