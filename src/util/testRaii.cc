// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/Raii.h"

// Boost unit test header
#define BOOST_TEST_MODULE RaiiTest
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;

using namespace std;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(RaiiCounterTest) {
    LOGS_INFO("RaiiCounterTest begins");

    auto counterA = util::RaiiCounter<int>::create();
    BOOST_REQUIRE(counterA->getCount() == 0);
    BOOST_REQUIRE(counterA->getName() == "none");

    auto counterB = util::RaiiCounter<unsigned int>::create("counterB");
    BOOST_REQUIRE(counterB->getCount() == 0);
    BOOST_REQUIRE(counterB->getName() == "counterB");

    uint64_t const cStart = 89;
    auto counterC = util::RaiiCounter<uint64_t>::create(cStart);
    BOOST_REQUIRE(counterC->getCount() == cStart);
    BOOST_REQUIRE(counterC->getName() == "none");

    int64_t const dStart = 9876;
    auto counterD = util::RaiiCounter<int64_t>::create("counterD", dStart);
    BOOST_REQUIRE(counterD->getCount() == dStart);
    BOOST_REQUIRE(counterD->getName() == "counterD");

    {
        auto raiiA = counterA->createRaii();
        BOOST_REQUIRE(counterA->getCount() == 1);

        auto raiiB = counterB->createRaii();
        BOOST_REQUIRE(counterB->getCount() == 1);

        auto raiiC = counterC->createRaii();
        BOOST_REQUIRE(counterC->getCount() == cStart + 1);

        auto raiiD = counterD->createRaii();
        BOOST_REQUIRE(counterD->getCount() == dStart + 1);
    }
    BOOST_REQUIRE(counterA->getCount() == 0);
    BOOST_REQUIRE(counterB->getCount() == 0);
    BOOST_REQUIRE(counterC->getCount() == cStart);
    BOOST_REQUIRE(counterD->getCount() == dStart);
}

BOOST_AUTO_TEST_SUITE_END()
