// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
  * @file testCssException.cc
  *
  * @brief Unit test for CssException.
  *
  * @Author Jacek Becla, SLAC
  */


// standard library imports
#include <iostream>

// boost
#define BOOST_TEST_MODULE MyTest
#include <boost/test/included/unit_test.hpp>
#include <boost/lexical_cast.hpp>

// local imports
#include "CssException.h"

using std::cout;
using std::endl;

namespace lsst {
namespace qserv {
namespace css {


struct CssExFixture {
    CssExFixture(void) {
    };

    ~CssExFixture(void) {
    };
};

BOOST_FIXTURE_TEST_SUITE(CssExTest, CssExFixture)

BOOST_AUTO_TEST_CASE(testAll) {
    try {    
        throw CssException(CssException::KEY_DOES_NOT_EXIST);
    } catch (CssException& e) {
        BOOST_CHECK_EQUAL(e.errCode(), CssException::KEY_DOES_NOT_EXIST);
    }

    try {
        throw CssException(CssException::DB_DOES_NOT_EXIST);
    } catch (CssException& e) {
        BOOST_CHECK_EQUAL(e.errCode(), CssException::DB_DOES_NOT_EXIST);
    }

    try {
        throw CssException(CssException::DB_DOES_NOT_EXIST, "myDB");
    } catch (CssException& e) {
        BOOST_CHECK_EQUAL(e.errCode(), CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_SUITE_END()

}}} // namespace lsst::qserv::css
