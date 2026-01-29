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

// Class header
#include "protojson/PwHideJson.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.PwHideJson");
}  // namespace

namespace lsst::qserv::protojson {

nlohmann::json PwHideJson::hide(nlohmann::json const& in) const {
    try {
        nlohmann::json js(in);
        for (auto const& key : keySet) {
            auto iter = js.find(key);
            if (iter != js.end()) {
                *iter = mask;
            }
        }
        return js;
    } catch (...) {
        /// This should never happen, but this function is only expected to
        /// be used in rare errors. It just shouldn't crash the program
        /// under any circumstances.
        nlohmann::json jsthrew({"PwHideJson::hide threw something", 0});
        return jsthrew;
    }
}

}  // namespace lsst::qserv::protojson
