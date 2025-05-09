// -*- LSST-C++ -*-
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

// System headers
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "util/ResultFileName.h"

// Boost unit test header
#define BOOST_TEST_MODULE ResultFileName
#include <boost/test/unit_test.hpp>

// Third party headers
#include "boost/filesystem.hpp"

namespace fs = boost::filesystem;
namespace test = boost::test_tools;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ResultFileNameTest) {
    LOGS_INFO("ResultFileNameTest");

    lsst::qserv::qmeta::CzarId const czarId = 1;
    lsst::qserv::QueryId const queryId = 2;
    lsst::qserv::UberJobId const ujId = 3;

    std::string const name2parse = std::to_string(czarId) + "-" + std::to_string(queryId) + "-" +
                                   std::to_string(ujId) + lsst::qserv::util::ResultFileName::fileExt;

    BOOST_CHECK_NO_THROW({
        lsst::qserv::util::ResultFileName const file(name2parse);
        BOOST_CHECK_EQUAL(file.fileName(), name2parse);
        BOOST_CHECK_EQUAL(file.czarId(), czarId);
        BOOST_CHECK_EQUAL(file.queryId(), queryId);
        BOOST_CHECK_EQUAL(file.ujId(), ujId);
    });

    BOOST_CHECK_NO_THROW({
        lsst::qserv::util::ResultFileName const file("base-folder/" + name2parse);
        BOOST_CHECK_EQUAL(file.fileName(), name2parse);
        BOOST_CHECK_EQUAL(file.czarId(), czarId);
        BOOST_CHECK_EQUAL(file.queryId(), queryId);
        BOOST_CHECK_EQUAL(file.ujId(), ujId);
    });

    BOOST_CHECK_NO_THROW({
        lsst::qserv::util::ResultFileName const file(fs::path("base-folder/" + name2parse));
        BOOST_CHECK_EQUAL(file.fileName(), name2parse);
        BOOST_CHECK_EQUAL(file.czarId(), czarId);
        BOOST_CHECK_EQUAL(file.queryId(), queryId);
        BOOST_CHECK_EQUAL(file.ujId(), ujId);
    });

    BOOST_CHECK_NO_THROW({
        lsst::qserv::util::ResultFileName const file(czarId, queryId, ujId);
        BOOST_CHECK_EQUAL(file.fileName(), name2parse);
        BOOST_CHECK_EQUAL(file.czarId(), czarId);
        BOOST_CHECK_EQUAL(file.queryId(), queryId);
        BOOST_CHECK_EQUAL(file.ujId(), ujId);
    });

    BOOST_CHECK_THROW(
            { lsst::qserv::util::ResultFileName const file(std::string("1-2")); }, std::invalid_argument);

    BOOST_CHECK_THROW(
            { lsst::qserv::util::ResultFileName const file(std::string("a-2-3-4")); }, std::invalid_argument);
}

BOOST_AUTO_TEST_SUITE_END()
