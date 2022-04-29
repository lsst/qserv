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
#ifndef LSST_QSERV_REPLICA_CONFIGPARSERJSON_H
#define LSST_QSERV_REPLICA_CONFIGPARSERJSON_H

// System headers
#include <map>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/ConfigDatabase.h"
#include "replica/ConfigDatabaseFamily.h"
#include "replica/ConfigWorker.h"
#include "replica/ConfigurationSchema.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * The class for parsing and loading a configuration from the JSON object.
 */
class ConfigParserJSON {
public:
    ConfigParserJSON() = delete;
    ConfigParserJSON(ConfigParserJSON const&) = delete;
    ConfigParserJSON& operator=(ConfigParserJSON const&) = delete;

    /**
     * Construct the parser with references to the collections of the confituration
     * data to be filled in.
     * @param data The collection of the general parameters.
     * @param databaseFamilies The collection of the database family descriptors.
     * @param databases The collection of the database descriptors.
     */
    ConfigParserJSON(nlohmann::json& data, std::map<std::string, WorkerInfo>& workers,
                     std::map<std::string, DatabaseFamilyInfo>& databaseFamilies,
                     std::map<std::string, DatabaseInfo>& databases);

    /**
     * Parse the input object's content, validate it, and update it in the output
     * data structures passed into the class's constructor.
     * @param obj The input object to be parsed.
     */
    void parse(nlohmann::json const& obj);

private:
    /**
     * Validate and store a value of the value of a parameter.
     * @param dest The destination object.
     * @param source The source object.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter to test.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.
     */
    template <typename T>
    void _storeGeneralParameter(nlohmann::json& dest, nlohmann::json const& source,
                                std::string const& category, std::string const& param) {
        // Sanitize the input to ensure it matches schema requirements before
        // pushing the value into the configuration.
        ConfigurationSchema::validate<T>(category, param, source.get<T>());
        dest = source;
    }

    std::string const _context = "CONFIG-JSON-PARSER  ";

    // Input parameters

    nlohmann::json& _data;
    std::map<std::string, WorkerInfo>& _workers;
    std::map<std::string, DatabaseFamilyInfo>& _databaseFamilies;
    std::map<std::string, DatabaseInfo>& _databases;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGPARSERJSON_H
