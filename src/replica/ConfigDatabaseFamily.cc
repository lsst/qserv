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
#include "replica/ConfigDatabaseFamily.h"

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/ChunkNumber.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

DatabaseFamilyInfo::DatabaseFamilyInfo(json const& obj) {
    string const context = "DatabaseFamilyInfo::DatabaseFamilyInfo(json): ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    try {
        name = obj.at("name").get<string>();
        replicationLevel = obj.at("min_replication_level").get<size_t>();
        numStripes = obj.at("num_stripes").get<unsigned int>();
        numSubStripes = obj.at("num_sub_stripes").get<unsigned int>();
        overlap = obj.at("overlap").get<double>();
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
    chunkNumberValidator = make_shared<ChunkNumberQservValidator>(static_cast<int32_t>(numStripes),
                                                                  static_cast<int32_t>(numSubStripes));
}

json DatabaseFamilyInfo::toJson() const {
    json infoJson;
    infoJson["name"] = name;
    infoJson["min_replication_level"] = replicationLevel;
    infoJson["num_stripes"] = numStripes;
    infoJson["num_sub_stripes"] = numSubStripes;
    infoJson["overlap"] = overlap;
    return infoJson;
}

ostream& operator<<(ostream& os, DatabaseFamilyInfo const& info) {
    os << "DatabaseFamilyInfo: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
