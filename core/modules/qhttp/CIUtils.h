/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_QSERV_QHTTP_CIUTILS_H
#define LSST_QSERV_QHTTP_CIUTILS_H

// System headers
#include <functional>
#include <string>
#include <string.h>

// Third party headers
#include <boost/algorithm/string.hpp>

namespace lsst {
namespace qserv {
namespace qhttp {

//
//----- Case-insensitive hash and comparison functionals for std::string,
//      can be used for instantiation of case-insensitive standard containers.
//

struct ci_hash {
    size_t operator()(std::string const& key) const {
        return std::hash<std::string>()(boost::to_lower_copy(key));
    }
};

struct ci_pred {
    bool operator()(std::string const& lhs, std::string const& rhs) const {
        return boost::iequals(lhs, rhs);
    }
};

}}}  // namespace lsst::qserv::qhttp

#endif // LSST_QSERV_QHTTP_CIUTILS_H
