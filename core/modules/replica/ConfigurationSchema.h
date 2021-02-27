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
#include <memory>
#include <set>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"


// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Configuration;
namespace database {
namespace mysql {
    class Connection;
}}  // namespace database::mysql
}}} // namespace lsst::qserv::replica

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * This utility class ConfigurationSchema encaposulates the JSON and MySQL schemas
 * and operations over the schemas of the Configuration service. Both schemas are
 * put into this class to facilitate schema changes tracking, unit and integration
 * testing, as well as other actions that may involve the schemas, such as schema
 * evolution, schema verification, etc.
 *
 * @note This class is the primary source of the schemas for the given version.
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
     * Create and initialize the configuration database in MySQL.
     * @note The database is allowed to exist. If it's not then it will be created
     *   by this method. If the database is not empty then the behavior of the method
     *   would depends on a value of the optional flag 'reset'.
     * @note All operations in a scope of the database 
     * @note Besides 'std::logic_error' the method may throw other exceptions
     *   related to database operations.
     * @param configUrl A configuration URL for the new database.
     * @param reset The optional flag that if 'true' will tell the method to reset
     *   the content of the database by deleting all existing tables and populating
     *   the database with the current schema from scratch.
     * @return A pointer to the Configuration object initialized by the method.
     * @throws std::logic_error If the database exists and its not empty.
     */
    static std::shared_ptr<Configuration> create(std::string const& configUrl,
                                                 bool reset=false);

    /**
     * Upgrade persistent configuration schema up to the currnet one.
     * @throws std::logic_error If the persistent schema is not strictly less than
     *   the one that is expected by current implementation of the class.
     */
    static void upgrade(std::string const& configUrl);

    /**
     * Return the current MySQL schema and (optionally) the initialization
     * statements for the minimum set of teh default parameters required by
     * the Replication/Ingest system to operate.
     * @note This method is designed to work w/o having an open MySQL connection
     *   to generate proper quotes for SQL identifiers and values. Please consider
     *   the optional parameters 'idQuote' and 'valueQuote' allowing to explicitly
     *   specify the desired values of the quotes. The default values in the signature
     *   method are set to be consistent with the present configuration of
     *   the MySQL/MariaDB services of Qserv and the Replication/Ingest system.
     * @param includeInitStatements If 'true' then add 'INSERT INTO ...' statements
     *   for initializing the default configuration parameters.
     * @param idQuote The quotation symbol for MySQL identifiers.
     * @param valueQuote The quotation string for MySQL values.
     * @return An ordered collection of the MySQL statements for creating schema and
     *   initializing configuration parameters. The order of statements takes into
     *   accout dependencies between the tables (such as FK->PK relationships).
     */
    static std::vector<std::string> schema(bool includeInitStatements=true,
                                           std::string const& idQuote="`",
                                           std::string const& valueQuote="'");

    /**
     * Serialize a primitive JSON object into a non-quoted string.
     * @param context A context from which the operation was initiated. It's used for
     *   error reporting purposes.
     * @param obj A JSON object to be serialized.
     * @throws std::invalid_argument If the input object can't be serialized into a string.
     */
    static std::string json2string(std::string const& context, nlohmann::json const& obj);

private:
    /**
     * Make the table creation statement from the specified table definition.
     * @note If a valid connection is not provided then the quotations strings will be used
     *   for generating SQL statements.
     * @param obj Table definition.
     * @param conn An optional MySQL connection that is required to take into account
     *   various locales, etc. set for the MySQL server when generating the statement.
     * @param idQuote The quotation string for MySQL identifiers.
     * @param valueQuote The quotation string for MySQL values.
     */
    static std::string _tableCreateStatement(nlohmann::json const& obj,
                                             std::shared_ptr<database::mysql::Connection> const& conn,
                                             std::string const& idQuote="`");

    static std::vector<std::string> _defaultConfigStatements(
            std::shared_ptr<database::mysql::Connection> const& conn,
            std::string const& idQuote="`",
            std::string const& valueQuote="'");

    /**
     * Store the default configuration parameters in the database.
     * @note If a valid connection is not provided then the quotations string will be used
     *   for generating SQL statements.
     * @param conn An optional MySQL connection that is required to access the database.
     * @param idQuote The quotation string for MySQL identifiers.
     * @param valueQuote The quotation string for MySQL values.
     */
    static void _storeDefaultConfig(std::shared_ptr<database::mysql::Connection> const& conn,
                                    std::string const& idQuote="`",
                                    std::string const& valueQuote="'");

    /// The JSON schema of the transient configuration.
    static nlohmann::json const _schemaJson;

    /// Persistent schemas of the MySQL tables.
    static nlohmann::json const _schemaMySQL;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H
