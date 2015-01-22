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

#include "MultiError.h"
#include <algorithm>
#include <iterator>
#include <sstream>

namespace lsst {
namespace qserv {
namespace util {

/** Return a string representation of the object
 *
 * @return a string representation of the object
 */
std::string MultiError::toString() const {
    std::ostringstream oss;

    if (!this->empty()) {
        std::ostream_iterator<Error> string_it(oss, "\n");
        if (this->size()>1) oss << "Multi-error:\n";
        std::copy(this->begin(), this->end(), string_it);
    }
    return oss.str();
}

/** Overload output operator for this class
 *
 * @param out
 * @param multiError
 * @return an output stream
 */
std::ostream& operator<<(std::ostream &out,
        MultiError const& multiError) {
    out << multiError.toString();
    return out;
}
}
}
} // lsst::qserv::util
