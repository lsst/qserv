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
 * @brief ErrorStack stores a generic throwable errors message list
 * Error is a template type whose operator << is used for output.
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#include "ErrorStack.h"

// System headers
#include <algorithm>
#include <iterator>
#include <sstream>

namespace lsst {
namespace qserv {
namespace util {

void ErrorStack::push(Error const& error) {
    _errors.push_back(error);
}

std::string ErrorStack::toString() const {
    std::ostringstream oss;

    if (!_errors.empty()) {
        std::ostream_iterator<Error> string_it(oss, "\n");
        if (_errors.size()>1) oss << "Multi-error:\n";
        std::copy(_errors.begin(), _errors.end(), string_it);
    }
    return oss.str();
}

std::ostream& operator<<(std::ostream &out,
        ErrorStack const& errorStack) {
    out << errorStack.toString();
    return out;
}
}
}
} // lsst::qserv::util
