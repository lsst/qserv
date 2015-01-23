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
 * @file
 *
 * @brief Simple testing for class ErrorStack
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */


// System headers
#include <iostream>
#include <sstream>
#include <string>


// Third-party headers

// Qserv headers
#include "util/ErrorStack.h"

namespace util = lsst::qserv::util;

int main()
{

    std::string out;
    util::ErrorStack errorStack;

    for( int errCode = 10; errCode < 20; errCode = errCode + 1 ) {
        std::stringstream ss;
        ss << "Error code is: " << errCode;
        std::string errMsg = ss.str();
        util::Error error(errCode, errMsg);
        errorStack.push(error);
    }

    std::cout << errorStack;
}
