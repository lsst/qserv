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

#include "cconfig/DataManagementEvent.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {

string parseDatabase(json const& jsonObj, string const& context) {
    if (!jsonObj.contains("database")) {
        throw invalid_argument(context + ": Missing required field 'database'");
    }
    return jsonObj.at("database").get<string>();
}

string parseTable(json const& jsonObj, string const& context) {
    if (!jsonObj.contains("table")) {
        throw invalid_argument(context + ": Missing required field 'table'");
    }
    return jsonObj.at("table").get<string>();
}

}  // namespace

namespace lsst::qserv::cconfig {

string DataManagementEvent::type2str(Type type) {
    switch (type) {
        case Type::NONE:
            return "NONE";
        case Type::CHUNK_MAP_REBUILT:
            return "CHUNK_MAP_REBUILT";
        case Type::DATABASE_PUBLISHED:
            return "DATABASE_PUBLISHED";
        case Type::DATABASE_DELETED:
            return "DATABASE_DELETED";
        case Type::TABLE_DELETED:
            return "TABLE_DELETED";
    }
    throw invalid_argument("Unknown DataManagementEvent::Type: " + std::to_string(static_cast<int>(type)));
}

DataManagementEvent::Type DataManagementEvent::str2type(string const& str) {
    string const context = "DataManagementEvent::" + string(__func__);
    if (str == "NONE") return Type::NONE;
    if (str == "CHUNK_MAP_REBUILT") return Type::CHUNK_MAP_REBUILT;
    if (str == "DATABASE_PUBLISHED") return Type::DATABASE_PUBLISHED;
    if (str == "DATABASE_DELETED") return Type::DATABASE_DELETED;
    if (str == "TABLE_DELETED") return Type::TABLE_DELETED;
    throw invalid_argument(context + ": unknown DataManagementEvent::Type: '" + str + "'");
}

DataManagementEvent DataManagementEvent::fromJson(json const& jsonObj) {
    string const context = "DataManagementEvent::" + string(__func__);
    DataManagementEvent event;
    if (!jsonObj.is_object()) throw invalid_argument(context + ": JSON object expected");
    if (!jsonObj.contains("timestamp"))
        throw invalid_argument(context + ": Missing required field 'timestamp'");
    event.timestamp =
            chrono::system_clock::time_point(chrono::milliseconds(jsonObj.at("timestamp").get<long long>()));
    if (!jsonObj.contains("type")) throw invalid_argument(context + ": Missing required field 'type'");
    event.type = DataManagementEvent::str2type(jsonObj.at("type").get<string>());
    switch (event.type) {
        case Type::NONE:
            break;
        case Type::CHUNK_MAP_REBUILT:
            break;
        case Type::DATABASE_PUBLISHED:
            event.database = ::parseDatabase(jsonObj, context);
            break;
        case Type::DATABASE_DELETED:
            event.database = ::parseDatabase(jsonObj, context);
            break;
        case Type::TABLE_DELETED:
            event.database = ::parseDatabase(jsonObj, context);
            event.table = ::parseTable(jsonObj, context);
            break;
        default:
            throw invalid_argument(context + ": Unknown DataManagementEvent::Type");
    }
    return event;
}

json DataManagementEvent::toJson() const {
    return {{"timestamp", chrono::duration_cast<chrono::milliseconds>(timestamp.time_since_epoch()).count()},
            {"type", DataManagementEvent::type2str(type)},
            {"database", database},
            {"table", table}};
}

}  // namespace lsst::qserv::cconfig
