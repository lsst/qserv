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

// Qserv headers
#include "replica/util/SqlSchemaUtils.h"

// System headers
#include <fstream>

// Third party headers
#include "boost/filesystem.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE SqlSchemaUtils
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
namespace fs = boost::filesystem;
using namespace lsst::qserv::replica;

namespace {
string makeTempFileName(string const& baseFolder = "/tmp", string const& prefix = "SqlSchemaUtils-",
                        string const& suffix = "columns") {
    auto const p = fs::path(baseFolder) / fs::unique_path(prefix + "%%%%_%%%%_%%%%_%%%%." + suffix);
    return p.string();
}
}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SqlSchemaUtilsTest) {
    LOGS_INFO("SqlSchemaUtils test begins");

    BOOST_REQUIRE_NO_THROW({
        auto const filePath = ::makeTempFileName();
        ofstream f(filePath);
        BOOST_CHECK(f.is_open());
        f << "a INT\n";
        f << "b TEXT NOT NULL\n";
        f.close();
        auto const coldefs = SqlSchemaUtils::readFromTextFile(filePath);
        BOOST_CHECK(coldefs.size() == 2);

        auto itr = coldefs.cbegin();
        BOOST_CHECK(itr->name == "a");
        BOOST_CHECK(itr->type == "INT");

        ++itr;
        BOOST_CHECK(itr->name == "b");
        BOOST_CHECK(itr->type == "TEXT NOT NULL");

        fs::remove(fs::path(filePath));
    });

    BOOST_REQUIRE_NO_THROW({
        auto const filePath = ::makeTempFileName();
        ofstream f(filePath);
        BOOST_CHECK(f.is_open());
        f << "a 0 1\n";
        f << "b 10 0\n";
        f.close();
        auto const coldefs = SqlSchemaUtils::readIndexSpecFromTextFile(filePath);
        BOOST_CHECK(coldefs.size() == 2);

        auto itr = coldefs.cbegin();
        BOOST_CHECK(itr->name == "a");
        BOOST_CHECK(itr->length == 0);
        BOOST_CHECK(itr->ascending);

        ++itr;
        BOOST_CHECK(itr->name == "b");
        BOOST_CHECK(itr->length == 10);
        BOOST_CHECK(not itr->ascending);

        fs::remove(fs::path(filePath));
    });

    LOGS_INFO("SqlSchemaUtils test ends");
}

BOOST_AUTO_TEST_SUITE_END()
