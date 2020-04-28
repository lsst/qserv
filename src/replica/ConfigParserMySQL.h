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
#ifndef LSST_QSERV_REPLICA_CONFIGPARSERMYSQL_H
#define LSST_QSERV_REPLICA_CONFIGPARSERMYSQL_H

// System headers
#include <map>
#include <stdexcept>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/ConfigDatabase.h"
#include "replica/ConfigDatabaseFamily.h"
#include "replica/ConfigWorker.h"
#include "replica/ConfigurationSchema.h"
#include "replica/DatabaseMySQL.h"
    
// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * The class for parsing and loading the persistent configuration stored in MySQL.
 */
class ConfigParserMySQL {
public:
    ConfigParserMySQL() = delete;
    ConfigParserMySQL(ConfigParserMySQL const&) = delete;
    ConfigParserMySQL& operator=(ConfigParserMySQL const&) = delete;

    /**
     * Construct the parser with references to the collections of the confituration
     * data to be filled in.
     * @param conn A connection to the MySQL service for parsing the parameters.
     * @param data The collection of the general parameters.
     * @param databaseFamilies The collection of the database family descriptors.
     * @param databases The collection of the database descriptors.
     */
    ConfigParserMySQL(database::mysql::Connection::Ptr const& conn,
                      nlohmann::json& data,
                      std::map<std::string, WorkerInfo>& workers,
                      std::map<std::string, DatabaseFamilyInfo>& databaseFamilies,
                      std::map<std::string, DatabaseInfo>& databases);

    /**
     * Parse and load everything.
     * @throws std::runtime_error If a required field has NULL.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.
     */
    void parse();

private:

    /// Parse general (category/parameter) parameters.
    void _parseGeneral();

    /**
     * Parse a collection of workers.
     *
     * When parsing optional ports and data folders use default values from
     * the collection worker defaults. For the optional host names (all but
     * the name of a host where the replication service 'svc' runs) use
     * the host name of the 'svc' service.
     */
    void _parseWorkers();

    /// Parse a collection of the database families.
    void _parseDatabaseFamilies();

    /// Parse a collection of the databases.
    void _parseDatabases();

    /**
     * Extract a value of the general parameter into the requested type, sanitize the value
     * if needed, and store it in the transent state.
     * @throws std::runtime_error If a required field has NULL.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.
     */
    template <typename T>
    void _storeGeneralParameter(std::string const& category, std::string const& param) {
        T value;
        if (!_row.get("value", value)) {
            throw std::runtime_error(
                    _context + " NULL is not allowed, category:'" + category
                    + "' param: '" + param + "'.");
        }
        // Sanitize the input to ensure it matches schema requirements before
        // pushing the value into the configuration.
        ConfigurationSchema::validate<T>(category, param, value);
        _data[category][param] = value;
    }

    template <typename T>
    T _parseParam(std::string const& name) {
        T value;
        if (_row.get(name, value)) return value;
        throw std::runtime_error(
                _context + " the spec field '" + name + "' is not allowed to be NULL");
    }

    template <typename T>
    T _parseParam(std::string const& name, T const& defaultValue) {
        T value;
        if (_row.get(name, value)) return value;
        return defaultValue;
    }

    template <typename T>
    T _parseParam(std::string const& name, nlohmann::json const& defaults) {
        T value;
        if (_row.get(name, value)) return value;
        return defaults.at(name).get<T>();
    }

    std::string const _context =  "CONFIG-MYSQL-PARSER  ";

    // Input parameters

    database::mysql::Connection::Ptr const _conn;
    nlohmann::json& _data;
    std::map<std::string, WorkerInfo>& _workers;
    std::map<std::string, DatabaseFamilyInfo>& _databaseFamilies;
    std::map<std::string, DatabaseInfo>& _databases;

    /// The current row of the MySQL result set is used for extracting
    /// values of parameters.
    database::mysql::Row _row;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGPARSERMYSQL_H
