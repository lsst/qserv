// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "util/Error.h"

// System headers
#include <sstream>

namespace lsst {
namespace qserv {
namespace util {

/** Return a string representation of the object
 *
 * @return a string representation of the object
 */
std::string Error::toString() const {
    std::ostringstream str;
    str << *this;
    return str.str();
}

/** Overload output operator for this class
 *
 * @param out
 * @param multiError
 * @return an output stream
 */
std::ostream& operator<<(std::ostream &out, Error const& error) {
    out << "[" << error._code << "] " << error._msg;
    return out;
}

}}} // namespace lsst::qserv::util
