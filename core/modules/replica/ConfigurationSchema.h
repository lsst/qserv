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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H
#define LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H

// System headers
#include <map>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"


// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * This utility class ConfigurationSchema provides methods returning known JSON schemas of
 * the Configuration service.
 */
class ConfigurationSchema {
public:
    /// The current version number required by the application.
    static int const version;

    /// @return A documentation string for the specified parameter or the empty string
    ///   if none is available in the schema.
    static std::string description(std::string const& category, std::string const& param);

    /// @return A 'true' if the parameter can't be modified via the 'set' methods
    ///   of the Configuration class. This information is used by class Configuration
    ///   to validate the parameters.
    static bool readOnly(std::string const& category, std::string const& param);

    /// @return A 'true' if the parameter represents the security context (passwords,
    ///   authorization keys, etc.). Parameters possesing this attribute are supposed
    ///   to be used with care by the dependent automation tools to avoid exposing
    ///   sensitive information in log files, reports, etc.
    static bool securityContext(std::string const& category, std::string const& param);

    /// @return A 'true' if, depending on the actual type of the parameter, the empty
    ///   string (for strings) or zero value (for numeric parameters) is allowed.
    ///   This information is used by class Configuration to validate input values
    ///   of the parameters.
    static bool emptyAllowed(std::string const& category, std::string const& param);

    /// @return The default configuration data as per the current JSON schema to be loaded
    ///   into the transent state of the class Configuration upon its initialization.
    static nlohmann::json defaultConfigData();

    /// @return The configuration data for the unit testing. The data is compatible with
    ///   the current JSON configuration schema. In addition to the overwritten default
    ///   of the general parameters it also containers tst definitions for the group data,
    ///   that includes workers, database families and databases.
    static nlohmann::json testConfigData();

    /**
     * The directory method for locating categories and parameters within
     * the given category known to the current implementation.
     * @note The method only returns the so called "general" categories
     *   of primitive parameters that exclude workers, databa families,
     *   databases, etc.
     * @return A collection of categories and parameters within the given category.
     *   The name of a category would be the dictionary key, and a value of
     *   the dictionary will contains a set of the parameter names within
     *   the corresponding category.
     */
    static std::map<std::string, std::set<std::string>> parameters();

    /**
     * Serialize a primitive JSON object into a non-quoted string.
     * @param context A context from which the operation was initiated. It's used for
     *   error reporting purposes.
     * @param obj A JSON object to be serialized.
     * @throws std::invalid_argument If the input object can't be serialized into a string.
     */
    static std::string json2string(std::string const& context, nlohmann::json const& obj);

private:
    /// The schema of the transient configuration.
    static nlohmann::json const _schemaJson;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H
