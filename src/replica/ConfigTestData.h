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
#ifndef LSST_QSERV_REPLICA_CONFIGTESTDATA_H
#define LSST_QSERV_REPLICA_CONFIGTESTDATA_H

// System headers
#include <map>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * This utility class ConfigTestData provides inputs for testing the Configuration
 * service.
 */
class ConfigTestData {
public:
    /// @return A collection of the general configuration parameters. The parameters
    ///   are compatible with the current JSON configuration schema.
    static std::map<std::string, std::set<std::string>> parameters();

    /// @return The configuration data for the unit testing. The data is compatible with
    ///   the current JSON configuration schema. In addition to the overwritten default
    ///   of the general parameters it also containers test definitions for the group data,
    ///   that includes workers, database families and databases.
    static nlohmann::json data();
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGTESTDATA_H
