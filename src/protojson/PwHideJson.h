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
#ifndef LSST_QSERV_PROTOJSON_PWHIDEJSON_H
#define LSST_QSERV_PROTOJSON_PWHIDEJSON_H

// System headers
#include <string>
#include <set>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers

namespace lsst::qserv::protojson {

/// Return a new json object where the top level values of keys in `keySet` are
/// replaced with the `mask` (default "-").
class PwHideJson {
public:
    PwHideJson() = default;

    /// Return a copy of `in` where top level secret keys are set to mask.
    /// TODO: make recursive
    nlohmann::json hide(nlohmann::json const& in) const;

    std::set<std::string> keySet{"auth_key", "password", "pw"};
    std::string mask{"-"};
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_PWHIDEJSON_H
