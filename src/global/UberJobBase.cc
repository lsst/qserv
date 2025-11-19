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
 * MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "global/UberJobBase.h"

// System headers
#include <sstream>

// Third-party headers

// Qserv headers

// LSST headers

using namespace std;

namespace lsst::qserv {

std::ostream& UberJobBase::dumpOS(std::ostream& os) const {
    os << _idStr;
    return os;
}

std::string UberJobBase::dump() const {
    std::ostringstream os;
    dumpOS(os);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, UberJobBase const& uj) { return uj.dumpOS(os); }

}  // namespace lsst::qserv
