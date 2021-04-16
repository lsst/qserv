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
#ifndef LSST_QSERV_REPLICA_CONFIGURATION_H
#define LSST_QSERV_REPLICA_CONFIGURATION_H

/**
 * This header defines the class Configuration and a number of
 * other relevant classes, which represent a public interface to
 * the Configuration service of the Replication System.
 */

// System headers
#include <cstdint>
#include <list>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"
#include "replica/ConfigDatabase.h"
#include "replica/ConfigDatabaseFamily.h"
#include "replica/ConfigWorker.h"
#include "replica/ConfigurationExceptions.h"
#include "replica/ConfigurationSchema.h"
#include "replica/DatabaseMySQLExceptions.h"
#include "replica/DatabaseMySQLTypes.h"
#include "util/Mutex.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {
    class Connection;
    class ConnectionParams;
}}}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace detail {

template <typename T>
struct TypeConversionTrait {
    static std::string to_string(T const val) { return std::to_string(val); }
    static const T& validate(std::string const& context, T const& val) {
        if (val == 0) throw std::invalid_argument(context + " the value can't be 0.");
        return val;
    }
};

template <>
struct TypeConversionTrait<std::string> {
    static std::string const& to_string(std::string const& val) { return val; }
    static const std::string& validate(std::string const& context, std::string const& val) {
        if (val.empty()) throw std::invalid_argument(context + " the string value can't be empty.");
        return val;
    }
};
}   // namespace detail

/**
 * Class Configuration is the main API class that provide configuration services
 * for the components of the Replication system.
 * @note The optional flag 'updatePersistentState' found in signatures of several
 *   "setter" methods will result in in propagating requested changes to the persistent
 *   store as well. The default value is set to 'true' since this would be a desired
 *   effect of the operations.
 * @note Exceptions mentioned in the documentation of the class's methods may not be
 *   complete. Additional exceptions may be thrown depending on a presence of a persistent
 *   backend for the configuration (such MySQL), Those which are mentioned explicitly are
 *   mostly related to incorrect/inconsistent values that are detected directly by in
 *   the implementation of the methods.
 */
class Configuration {
public:
    typedef std::shared_ptr<Configuration> Ptr;

    // ----------------------------------------------------------------------
    // The static API. It's designed to be used before loading the content of
    // the Configuration object in order to bootstrap an application before
    // the rest of the configuration will be known.
    // Some parameters mentioned in this section also represent security
    // context, therefore they can't be found in an external configuration.
    // ----------------------------------------------------------------------

    /**
     * The static factory method will create an object, initialize its state with
     * the default values of the configuration parameters, then update the state
     * from the given JSON object.
     * @note Configuration objects created by this method won't have any persistent
     *   backend should any changes to the transient state be made.
     * @param obj The input configuration parameters. The object is optional.
     *   If it's not given (the default value), or if it's empty then no changes
     *   will be made to the default transient state.
     * @throw std::runtime_error If the input configuration is not consistent
     *   with the transient schema.
     */
    static Ptr load(nlohmann::json const& obj=nlohmann::json::object());

    /**
     * The static factory method will create a new object and initialize its content
     * from the following source:
     * @code
     *   mysql://[user][:password][@host][:port][/database]
     * @code
     * @note Configuration objects initialized from MySQL would rely on MySQL as
     *   the persistent backend for any requests to update the state of the transient
     *   parameters. A connection object to the MySQL service will be initialized
     *   as the corresponding data member of the class.
     * @param configUrl The configuration source.
     * @throw std::invalid_argument If the URL has unsupported scheme or it
     *   couldn't be parsed.                          
     * @throw std::runtime_error If the input configuration is not consistent
     *   with the transient schema.
     */
    static Ptr load(std::string const& configUrl);

    /**
     * Return a connection object for the czar's MySQL service with the name of
     * a database optionally rewritten from the one stored in the corresponding URL.
     * This is done for the sake of convenience of clients to ensure a specific
     * database is set as the default context.
     * @param database The optional name of a database to assume if a non-empty
     *   string was provided.
     * @return The parsed connection object with the name of the database optionally
     *   overwritten.
     */
    static database::mysql::ConnectionParams qservCzarDbParams(std::string const& database=std::string());

    /// @return A connection string for accessing Qserv czar's database.
    static std::string qservCzarDbUrl();

    /// @param url A connection string for accessing Qserv czar's database.
    static void setQservCzarDbUrl(std::string const& url);

    /**
     * Return a connection object for the worker's MySQL service with the name of
     * a database optionally rewritten from the one stored in the corresponding URL.
     * This is done for the sake of convenience of clients to ensure a specific
     * database is set as the default context.
     * @param database The optional name of a database to assume if a non-empty
     *   string was provided.
     * @return The parsed connection object with the name of the database optionally
     *   overwritten.
     */
     static database::mysql::ConnectionParams qservWorkerDbParams(std::string const& database=std::string());

    /**
     * This method is used by the Replication/Ingest system's workers when they need
     * to connect directly to the corresponding MySQL/MariaDB service of the corresponding
     * Qserv worker.
     * @return A connection string for accessing Qserv worker's database.
     */
    static std::string qservWorkerDbUrl();

    /// @param url The new connection URL to be set.
    static void setQservWorkerDbUrl(std::string const& url);

    /// @return the default mode for database reconnects.
    static bool databaseAllowReconnect();

    /**
     * Change the default value of a parameter defining a policy for handling
     * automatic reconnects to a database server. Setting 'true' will enable
     * reconnects.
     * @param value The new value of the parameter.
     */
    static void setDatabaseAllowReconnect(bool value);

    /// @return The default timeout for connecting to database servers.
    static unsigned int databaseConnectTimeoutSec();

    /**
     * Change the default value of a parameter specifying delays between automatic
     * reconnects (should those be enabled by the corresponding policy).
     * @param value The new value of the parameter (must be strictly greater than 0).
     * @throws std::invalid_argument If the new value of the parameter is 0.
     */
    static void setDatabaseConnectTimeoutSec(unsigned int value);

    /**
     * @return The default number of a maximum number of attempts to execute
     *   a query due to database connection failures and subsequent reconnects.
     */
    static unsigned int databaseMaxReconnects();

    /**
     * Change the default value of a parameter specifying the maximum number
     * of attempts to execute a query due to database connection failures and
     * subsequent reconnects (should they be enabled by the corresponding policy).
     * @param value The new value of the parameter (must be strictly greater than 0).
     * @throws std::invalid_argument If the new value of the parameter is 0.
     */
    static void setDatabaseMaxReconnects(unsigned int value);

    /// @return The default timeout for executing transactions at a presence
    ///   of server reconnects.
    static unsigned int databaseTransactionTimeoutSec();

    /**
     * Change the default value of a parameter specifying a timeout for executing
     * transactions at a presence of server reconnects.
     * @param value The new value of the parameter (must be strictly greater than 0).
     * @throws std::invalid_argument If the new value of the parameter is 0.
     */
    static void setDatabaseTransactionTimeoutSec(unsigned int value);

    /// @return the default mode for xrootd reconnects.
    static bool xrootdAllowReconnect();

    /**
     * Change the default value of a parameter defining a policy for handling
     * automatic reconnects to the xrootd server. Setting 'true' will enable
     * reconnects.
     * @param value The new value of the parameter.
     */
    static void setXrootdAllowReconnect(bool value);

    /// @return The default timeout for connecting to the xrootd servers.
    static unsigned int xrootdConnectTimeoutSec();

    /**
     * Change the default value of a parameter specifying the maximum interval
     * of time (seconds) to wait for connecting to the xrootd servers before
     * giving up (should this be enabled by the corresponding reconnect policy).
     * @param value The new value of the parameter (must be strictly greater than 0).
     * @throws std::invalid_argument If the new value of the parameter is 0.
     */
    static void setXrootdConnectTimeoutSec(unsigned int value);

    // -----------------
    // The instance API.
    // -----------------

    Configuration(Configuration const&) = delete;
    Configuration& operator=(Configuration const&) = delete;
    ~Configuration() = default;

    /**
     * Reload non-static parameters of the Configuration from the same source
     * they were originally read before.
     * @note If the object was initialized from a JSON object then
     *   the method will do nothing.
     */
    void reload();

    /**
     * Reload non-static parameters of the Configuration from the given JSON object.
     * @note If the previous state of the object was configured from a source having
     *   a persistent back-end (such as MySQL) then the association with the backend
     *   will be lost upon completion of the method.
     * @param obj The input configuration parameters.
     * @throw std::runtime_error If the input configuration is not consistent
     *   with expectations of the transient schema.
     */
    void reload(nlohmann::json const& obj);

    /**
     * Reload non-static parameters of the Configuration from an external source.
     * @param configUrl The configuration source,
     * @throw std::invalid_argument If the URL has unsupported scheme or it couldn't
     *   be parsed.
     * @throw std::runtime_error If the input configuration is not consistent with
     *   expectations of the transient schema.
     */
    void reload(std::string const& configUrl);
    
    /**
     * Construct the original (minus security-related info) path to
     * the configuration source.
     * @param showPassword If a value of the flag is 'false' then hash a password
     *   in the result.
     * @return The constructed path.
     */
    std::string configUrl(bool showPassword=false) const;

    /**
     * The directory method for locating categories and parameters within
     * the given category known to the current implementation.
     * @note The method only returns the so called "general" categories
     *   of primitive parameters that exclude workers, database families,
     *   databases, etc.
     * @return A collection of categories and parameters within the given category.
     *   The name of a category would be the key, and a value of
     *   the dictionary will contains a set of the parameter names within
     *   the corresponding category.
     */
    std::map<std::string, std::set<std::string>> parameters() const;

    /**
     * Return a value of the parameter found by its category and its name.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter within its category.
     * @return A value of the parameter of the requested type.
     * @throws std::invalid_argument If the parameter doesn't exist.
     * @throws ConfigTypeMismatch If the parameter has unexpected type.
     */
    template <typename T>
    T get(std::string const& category, std::string const& param) const {
        util::Lock const lock(_mtx, _context(__func__));
        nlohmann::json const obj = _get(lock, category, param);
        try {
            return obj.get<T>();
        } catch (nlohmann::json::type_error const& ex) {
            throw ConfigTypeMismatch(
                _context(__func__) + " failed to convert the parameter for category '"
                + category + "', and param '" + param + "', expected type: '" + std::string(obj.type_name())
                + "', ex: " + std::string(ex.what()) + "'.");
        } catch (std::exception const&) {
            throw;
        }
    }

    /**
     * Get a value of a parameter as a string.
     * @see Configuration::get()
     */
    std::string getAsString(std::string const& category, std::string const& name) const;

    /**
     * Set a new value of the parameter given its category and its name.
     * @note The parameter is required to exist.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter within its category.
     * @param val A new value of the parameter.
     * @throws std::logic_error For attempts to update values of the read-only parameters.
     * @throws std::invalid_argument If there was a problem during setting a value of the parameter.
     */
    template <typename T>
    void set(std::string const& category, std::string const& param, T const& val, bool updatePersistentState=true) {
        std::string const context_ = _context(__func__) + " category='" + category + "' param='" + param + "' ";
        util::Lock const lock(_mtx, context_);
        // Some parameters can't be updated using this interface.
        if (ConfigurationSchema::readOnly(category, param)) {
            throw std::logic_error(context_ + "the read-only parameters can't be updated via the API.");
        }
        // Validate the value in case if the schema enforces restrictions.
        ConfigurationSchema::validate(category, param, val);
        // Update the persistent (if allowed) and then the transient states.
        try {
            nlohmann::json& obj = _get(lock, category, param);
            if (updatePersistentState && (_connectionPtr != nullptr)) {
                // Convert the value into a string. This is possible because values of
                // the general parameters are stored as strings in the corresponding MySQL table.
                _set(category, param,  detail::TypeConversionTrait<T>::to_string(val));
            }
            obj = val;
        } catch (database::mysql::Error const& ex) {
            throw std::invalid_argument(
                context_ + " failed to update the persistent state of the parameter, ex: " + std::string(ex.what()));
        } catch (std::exception const& ex) {
            throw std::invalid_argument(
                context_ + " failed to set a new value of the parameter, ex: " + std::string(ex.what()) + "'.");
        }
    }

    /**
     * Parse and set parameter value from a string. The string will be converted
     * into the same type of the parameter stored in the transient state of the configuration.
     * @see Configuration::set()
     */
    void setFromString(std::string const& category, std::string const& param,
                       std::string const& val, bool updatePersistentState=true);

    /**
     * Return the names of known workers as per the selection criteria.
     * @param isEnabled Select workers which are allowed to participate in the
     *   replication operations. If a value of the parameter is set
     *   to 'true' then the next flag 'isReadOnly' (depending on its state)
     *   would put further restrictions on the selected subset.
     *   Workers which are not 'enabled' are still known to the Replication
     *   system.
     * @param isReadOnly This flag will be considered only if 'isEnabled' is set
     *   to 'true'. The flag narrows down a subset of the 'enabled' workers which
     *   are either the read-only sources (if 'isReadOnly' is set to true')
     *   or the read-write replica sources/destinations.
     *   NOTE: no replica modification (creation or deletion) operations
     *   would be allowed against worker in the read-only state.
     * @return The names of known workers which have the specified properties
     *   as per input filters.
     */
    std::vector<std::string> workers(bool isEnabled=true,
                                     bool isReadOnly=false) const;

    /// @return The names of all known workers regardless of their statuses.
    std::vector<std::string> allWorkers() const;

    /// @return names of known database families
    std::vector<std::string> databaseFamilies() const;

    /// @param name The name of a family.
    /// @return 'true' if the specified database family is known to the configuration.
    /// @throw std::invalid_argument If the empty string passed as a value of the parameter.
    bool isKnownDatabaseFamily(std::string const& name) const;

    /**
     * @param name The name of a family.
     * @return The database family description.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if the specified entry was not found in the configuration.
     */
    DatabaseFamilyInfo databaseFamilyInfo(std::string const& name) const;

    /**
     * Register a new database family.
     * @param info Parameters of the family.
     * @return A description of the newly created database family.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter,
     *   or if the input descriptor has incorrect parameters (empty name,
     *   0 values of the numbers of stripes or sub-stripes, or 0 value of the replication level), or
     *   if the specified entry already exists in the configuration.
     */
    DatabaseFamilyInfo addDatabaseFamily(DatabaseFamilyInfo const& info);

    /**
     * Delete an existing family.
     * @note In order to maintain consistency of the persistent state this operation will
     *   also delete all dependent databases from both the transient and (if configured)
     *   from the persistent store as well.
     * @param name The name of a family.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if the specified entry was not found in the configuration.
     */
    void deleteDatabaseFamily(std::string const& name);

    /**
     * @param family The name of a database family.
     * @return The minimum number of chunk replicas for a database family.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if the specified entry was not found in the configuration.
     */
    size_t replicationLevel(std::string const& family) const;

    /**
     * The selector for the names of the known database.
     *
     * @param family The optional name of a database family.
     * @param allDatabases The optional flag which if set to 'true' will result
     *   in returning all known database entries regardless of their PUBLISHED
     *   status. Otherwise subset of databases as determined by the second flag
     *   'isPublished' will get returned.
     * @param isPublished The optional flag which is used if flag 'all' is set
     *   to 'false' to narrow a collection of databases returned by the method.
     * @return The names of known databases. A result of the method may be
     *   limited to a subset of databases belonging to the specified family.
     * @throw std::invalid_argument If the specified family (unless the empty string is passed
     *   into the method) was not found in the configuration.
     */
    std::vector<std::string> databases(std::string const& family=std::string(),
                                       bool allDatabases=false,
                                       bool isPublished=true) const;

    /**
     * @param name The name of a database.
     * @return 'true' if the specified database is known in the Configuration.
     */
    bool isKnownDatabase(std::string const& name) const;

    /**
     * @param name The name of a database.
     * @return A database descriptor.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if the specified entry was not found in the configuration.
     */
    DatabaseInfo databaseInfo(std::string const& name) const;

    /**
     * Register a new database.
     * @note The database status will be put into being un-published.
     * @param database The name of a database to be created.
     * @param family The name of a family the database will join.
     * @return A database descriptor of the newly created database.
     * @throw std::invalid_argument If the name of a family or other required parameters
     *   are the empty strings, or if the specified entry already exists in the configuration.
     */
    DatabaseInfo addDatabase(std::string const& database, std::string const& family);

    /**
     * Change database status to be published.
     * 
     * @param name The name of a database.
     * @return An updated database descriptor.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if no such database exists in the configuration.
     *  @throw std::logic_error If the database is already published.
     */
    DatabaseInfo publishDatabase(std::string const& name);

    /**
     * Change database status to be un-published.
     * @param name The name of a database.
     * @return An updated database descriptor.
     * @throw std::invalid_argument If the empty string passed as a value of the parameter, or
     *   if no such database exists in the configuration.
     *  @throw std::logic_error If the database is not yet published.
     */
    DatabaseInfo unPublishDatabase(std::string const& name);

    /**
     * Delete an existing database.
     * @param name The name of a database to be deleted.
     * @throw std::invalid_argument If the specified database doesn't exist, or
     *   if an empty string is passed as a parameter of the method.
     */
    void deleteDatabase(std::string const& name);

    /**
     * Register a new table with a database.
     * @param database The name of an existing database hosting the new table.
     * @param table The name of a new table to be registered.
     * @param isPartitioned A flag which is set 'true' if the table is partitioned.
     * @param columns An (optional) column definitions (name,type) of the table.
     * @param isDirectorTable An (optional) flag indicating if this is the "director"
     *   table of the catalog. Note there could be only one such table in a catalog,
     *   and this table must be "partitioned".
     * @param directorTableKey The (optional) name of a column in the "director"
     *   table which is used as an "object" identifier for astronomical objects.
     *   This parameter applies to the director tables only. And if provided (and
     *   allowed) the column must be found among the names of columns in a value
     *   of parameter "columns".
     * @param chunkIdColName The (optional) name of a column which stores identifiers
     *   of "chunks". This parameter applies to all "partitioned" tables, and if
     *   provided the column must be found among the names of columns in a value
     *   of parameter "columns".
     * @param subChunkIdColName The (optional) name of a column which stores
     *   identifiers of "sub-chunks". This parameter applies to all "partitioned"
     *   tables, and if provided the column must be found among the names of
     *   columns in a value of parameter "columns".
     * @param latitudeColName The (optional) name of a column which stores the latitude.
     * @param longitudeColName The (optional) name of a column which stores the longitude.
     * @return A database descriptor of the updated database.
     * @throw std::invalid_argument If the specified database doesn't exists, or
     *   if the table already exists, or if either of those parameters are the empty
     *   strings, or other required parameters have incorrect values or missing.
     */
    DatabaseInfo addTable(std::string const& database,
                          std::string const& table,
                          bool isPartitioned,
                          std::list<SqlColDef> const& columns=std::list<SqlColDef>(),
                          bool isDirectorTable=false,
                          std::string const& directorTableKey="objectId",
                          std::string const& chunkIdColName=lsst::qserv::CHUNK_COLUMN,
                          std::string const& subChunkIdColName=lsst::qserv::SUB_CHUNK_COLUMN,
                          std::string const& latitudeColName=std::string(),
                          std::string const& longitudeColName=std::string());

    /**
     * Delete an existing table.
     * @param database The name of an existing database hosting the table.
     * @param table The name of an existing table to be deleted.
     * @throw std::invalid_argument If the specified database doesn't exists, or
     *   if the table doesn't exist, or if either of those parameters are
     *   the empty strings.
     */
    DatabaseInfo deleteTable(std::string const& database,
                             std::string const& table);

    /**
     * @param name The name of a worker.
     * @return 'true' if the specified worker is known to the configuration.
     */
    bool isKnownWorker(std::string const& name) const;

    /**
     * @param name The name of a worker.
     * @return A worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    WorkerInfo workerInfo(std::string const& name) const;

    /**
     * Register a new worker in the Configuration.
     * @param workerInfo The worker description.
     * @return A worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    WorkerInfo addWorker(WorkerInfo const& info);

    /**
     * Completely remove the specified worker from the Configuration.
     * @param name The name of a worker affected by the operation.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    void deleteWorker(std::string const& name);

    /**
     * Disable the specified worker in the Configuration to exclude it from using
     * in any subsequent replication operations.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    WorkerInfo disableWorker(std::string const& name);

    /**
     * Update parameters of an existing worker in the transient store, and in
     * the persistent back-end as well (if any is associated with the Configuration object).
     * @param info The new worker descriptor.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    WorkerInfo updateWorker(WorkerInfo const& info);

    /// @param showPassword If a value of the flag is 'false' then hash a password in the result.
    /// @return The JSON representation of the object.
    nlohmann::json toJson(bool showPassword=false) const;

private:
    /**
     * @param func The optional name of a function (or other internal contex) that
     *   initiated the call.
     * @return The complete context string to be used for logging or other purposes.
     */
    static std::string _context(std::string const& func=std::string());

    /**
     * The light weight c-tor to initialize the default state of the configuration.
     * The rest of the state will get populated by specialized methods mentioned below.
     */
    Configuration();

    /**
     * Load from the transient JSON object. Parameters read from the object will
     * be applying to the internal state.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @param obj An object to be used as a source for updating the default values
     *   of the configuration parameters. Note that this source doesn't assume a presence
     *   of any persistent back-end for the configuration. If any such back-end existed
     *   then it would be disconnected upon the completion of the method.
     * @param reset The flag (if set to 'true') will trigger the internal state reset
     *   to the default values of the parameters before applying the input configuration.
     */
    void _load(util::Lock const& lock, nlohmann::json const& obj, bool reset);

    /**
     * Load from MySQL. Parameters read from the database will be applied
     * to the internal state.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @param configUrl The connection string to the MySQL database from which to read
     *   the configuration parameters. The database will become the persistent back-end
     *   for the configuration.
     * @param reset The flag (if set to 'true') will trigger the internal state reset
     *   to the default values of the parameters before applying the input configuration.
     */
    void _load(util::Lock const& lock, std::string const& configUrl, bool reset);

    /// @param lock The lock on '_mtx' to be acquired prior to calling the method.
    /// @param showPassword If a value of the flag is 'false' then hash a password in the result.
    /// @return The JSON representation of the object.
    nlohmann::json _toJson(util::Lock const& lock, bool showPassword=false) const;

    /**
     * Return a JSON object representing a parameter.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter within its category.
     * @return A 'const' reference to a JSON object encapsulating the parameter's value and its type.
     * @throws std::invalid_argument If the parameter doesn't exist.
     */
    nlohmann::json const& _get(util::Lock const& lock,
                               std::string const& category,
                               std::string const& param) const;

    /**
     * Return a modifiable JSON object representing a parameter.
     * @note The parameter is not required to exist at a time of the call. It would be
     *   created automatically with no specific value or type set.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter within its category.
     * @return A reference to a JSON object encapsulating the parameter's value and its type.
     */
    nlohmann::json& _get(util::Lock const& lock,
                         std::string const& category,
                         std::string const& param);

    /**
     * Update the general parameter parameter in the MySQL-based persistent store.
     * @note This method won't check for correctness of the parameter values. It's expected
     *   it has been done by a caller.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter within its category.
     * @param value A value to be updated that has been serialized into a string to break the type barrier.
     * @throws database::mysql::Error (or any derivatives of that exception)in case of any
     *   problems during the update.
     */
    void _set(std::string const& category,
              std::string const& param,
              std::string const& value) const;

    /**
     * Validate the input and add or update worker entry.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @return An updated worker description.
     */
    WorkerInfo _updateWorker(util::Lock const& lock,
                             WorkerInfo const& info);

    /**
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @return A family descriptor.
     * @throws std::invalid_argument If the name is empty or if no such entry exists.
     */
    DatabaseFamilyInfo& _databaseFamilyInfo(util::Lock const& lock, std::string const& name);

    DatabaseFamilyInfo const& _databaseFamilyInfo(util::Lock const& lock, std::string const& name) const {
        return const_cast<Configuration*>(this)->_databaseFamilyInfo(lock, name);
    }

    /**
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @return A database descriptor.
     * @throws std::invalid_argument If the name is empty or if no such entry exists.
     */
    DatabaseInfo& _databaseInfo(util::Lock const& lock, std::string const& name);

    DatabaseInfo const& _databaseInfo(util::Lock const& lock, std::string const& name) const {
        return const_cast<Configuration*>(this)->_databaseInfo(lock, name);
    }

    /**
     * Update database publishing flag in the transient store and (optionally, if applies)
     * in the persistent store as well.
     * @param lock The lock on '_mtx' to be acquired prior to calling the method.
     * @param name The name of a database entry to be updated.
     * @param publish The new state of the database.
     * @return An updated database descriptor.
     */
    DatabaseInfo& _publishDatabase(util::Lock const& lock,
                                   std::string const& name,
                                   bool publish);

    // Static parameters of the database connectors (read-write).

    static bool         _databaseAllowReconnect;
    static unsigned int _databaseConnectTimeoutSec;
    static unsigned int _databaseMaxReconnects;
    static unsigned int _databaseTransactionTimeoutSec;
    static std::string  _qservCzarDbUrl;
    static std::string  _qservWorkerDbUrl;
    static bool         _xrootdAllowReconnect;
    static unsigned int _xrootdConnectTimeoutSec;

    // For implementing static synchronized methods.
    static util::Mutex _classMtx;

    // A source of the configuration.
    std::string _configUrl;

    // These parameters  will be set for the MySQL back-end (if any).
    database::mysql::ConnectionParams _connectionParams;
    std::shared_ptr<database::mysql::Connection> _connectionPtr;

    // The transient state of the configuration (guarded by Mutex _mtx).
    nlohmann::json _data;
    std::map<std::string, WorkerInfo> _workers;
    std::map<std::string, DatabaseFamilyInfo> _databaseFamilies;
    std::map<std::string, DatabaseInfo> _databases;

    // For implementing synchronized methods.
    mutable util::Mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATION_H
