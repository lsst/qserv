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
#ifndef LSST_QSERV_REPLICA_CONFIGDATABASEFAMILY_H
#define LSST_QSERV_REPLICA_CONFIGDATABASEFAMILY_H

// System headers
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ChunkNumberValidator;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DatabaseFamilyInfo encapsulates various parameters describing
 * database families.
 */
class DatabaseFamilyInfo {
public:
    std::string  name;                  // The name of a database family
    size_t       replicationLevel = 0;  // The minimum replication level
    unsigned int numStripes = 0;        // The number of stripes (from the CSS partitioning configuration)
    unsigned int numSubStripes = 0;     // The number of sub-stripes (from the CSS partitioning configuration)
    double       overlap = 0.;          // The default overlap (radians) for tables that do not specify their own overlap

    std::shared_ptr<ChunkNumberValidator> chunkNumberValidator;     /// A validator for chunk numbers

    /**
     * Construct from a JSON object.
     * @param obj The optional object to be used of a source of the worker's state.
     * @throw std::invalid_argument If the input objces can't be parsed, or if it has
     *   incorrect schema.
     */
    explicit DatabaseFamilyInfo(nlohmann::json const& obj=nlohmann::json::object());

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, DatabaseFamilyInfo const& info);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGDATABASEFAMILY_H
