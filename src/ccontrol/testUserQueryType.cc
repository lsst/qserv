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


// Qserv headers
#include "ccontrol/UserQueryType.h"


// Boost unit test header
#define BOOST_TEST_MODULE UserQueryType
#include "boost/test/included/unit_test.hpp"

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>


using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(testCallQueryType) {
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("CALL QSERV_MANAGER foo"), true);
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("call qserv_manager foo"), true);
    BOOST_CHECK_EQUAL(ccontrol::UserQueryType::isCall("submit call qserv_manager foo"), false);
}

BOOST_AUTO_TEST_SUITE_END()
