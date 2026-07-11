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
#ifndef LSST_QSERV_CCONFIG_DATAMANAGEMENTEVENT_H
#define LSST_QSERV_CCONFIG_DATAMANAGEMENTEVENT_H

// System headers
#include <chrono>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::cconfig {

/**
 * Class DataManagementEvent is an abstraction for the events posted by the Replication system.
 * It contains information about the event type and any associated data.
 */
class DataManagementEvent {
public:
    enum class Type : int {
        NONE = 0,
        CHUNK_MAP_REBUILT,
        DATABASE_PUBLISHED,
        DATABASE_DELETED,
        TABLE_DELETED
    };

    static std::string type2str(Type type);
    static Type str2type(std::string const& str);

    /**
     * Construct a DataManagementEvent object from a JSON representation.
     *
     * @param jsonObj The JSON object containing the event data.
     * @return A DataManagementEvent object.
     * @throws std::invalid_argument if the JSON object is missing required fields or contains invalid data.
     */
    static DataManagementEvent fromJson(nlohmann::json const& jsonObj);

    nlohmann::json toJson() const;

    // Timestamp of the event.
    std::chrono::system_clock::time_point timestamp = std::chrono::system_clock::now();

    // Mandatory attribute
    Type type = Type::NONE;

    // Optional attributes which depend on the event type.
    std::string database;
    std::string table;
};

}  // namespace lsst::qserv::cconfig

#endif  // LSST_QSERV_CCONFIG_DATAMANAGEMENTEVENT_H
