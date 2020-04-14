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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONIFACE_H
#define LSST_QSERV_REPLICA_CONFIGURATIONIFACE_H

/**
 * This header defines an interface class Configuration and a number of
 * other relevant classes, which represent a public interface to
 * the Configuration service of the Replication System. Specific implementations
 * of the service's interface are found in separate headers and source files.
 */

// System headers
#include <cstdint>
#include <iosfwd>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"


// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"

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
 * Class WorkerInfo encapsulates various parameters describing a worker.
 */
class WorkerInfo {
public:
    std::string name;           /// The logical name of a worker

    bool isEnabled  = true;     /// The worker is allowed to participate in the replication operations
    bool isReadOnly = false;    /// The worker can only serve as a source of replicas.
                                /// New replicas can't be placed on it.

    std::string svcHost;        /// The host name (or IP address) of the worker service
    uint16_t    svcPort = 0;    /// The port number of the worker service

    std::string fsHost;         /// The host name (or IP address) of the file service for the worker
    uint16_t    fsPort = 0;     /// The port number for the file service for the worker

    std::string dataDir;        /// An absolute path to the data directory under which the MySQL
                                /// database folders are residing.

    std::string dbHost;         /// The host name (or IP address) of the database service for the worker
    uint16_t    dbPort = 0;     /// The port number of the worker database service
    std::string dbUser;         /// The name of a user account for connecting to the database service

    std::string loaderHost;     /// The host name (or IP address) of the ingest (loader) service
    uint16_t    loaderPort = 0; /// The port number of the ingest service

    std::string loaderTmpDir;   /// An absolute path to the temporary directory which would be used
                                /// by the service. The folder must be write-enabled for a user
                                /// under which the service will be run.

    std::string exporterHost;       /// The host name (or IP address) of the data exporting service
    uint16_t    exporterPort = 0;   /// The port number of the data exporting service

    std::string exporterTmpDir;     /// An absolute path to the temporary directory which would be used
                                    /// by the service. The folder must be write-enabled for a user
                                    /// under which the service will be run.

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, WorkerInfo const& info);

/**
 * Class DatabaseInfo encapsulates various parameters describing databases.
 */
class DatabaseInfo {
public:
    std::string name;    ///< The name of a database.
    std::string family;  ///< The name of the database family.

    bool isPublished = false;   ///< The status of the database.

    std::vector<std::string> partitionedTables; ///< The names of the partitioned tables.
    std::vector<std::string> regularTables;     ///< The list of fully replicated tables.

    /// Table schema (optional).
    std::map<std::string,                       // table name
             std::list<SqlColDef>> columns;

    /// @return The names of all tables.
    std::vector<std::string> tables() const {
        std::vector<std::string> result = partitionedTables;
        result.insert(result.begin(), regularTables.begin(), regularTables.end());
        return result;
    }

    std::string directorTable;      ///< The name of the Qserv "director" table if any.
    std::string directorTableKey;   ///< The name of the primary key column in the "director" table.

    // Names of special columns of the partitioned tables.

    std::string chunkIdColName;     ///< Same name for all partitioned tables.
    std::string subChunkIdColName;  //< Same name for all partitioned tables.

    std::map<std::string,                   // table name
             std::string> latitudeColName;  // latitude (declination) column name

    std::map<std::string,                   // table name
             std::string> longitudeColName; // longitude (right ascension) column name

    /// @param The name of a table to be located and inspected
    /// @return 'true' if the table was found and it's 'partitioned'
    /// @throw std::invalid_argument if no such table is known
    bool isPartitioned(std::string const& table) const;

    /// @param The name of a table to be located and inspected
    /// @return 'true' if the table was found and it's the 'partitioned' and the 'director' table
    /// @throw std::invalid_argument if no such table is known
    bool isDirector(std::string const& table) const;

    /// @return The table schema in format which is suitable for CSS.
    /// @throws std::out_of_range If the table is unknown.
    std::string schema4css(std::string const& table) const;

    /// @return The JSON representation of the object.
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, DatabaseInfo const& info);

/**
 * Class DatabaseFamilyInfo encapsulates various parameters describing
 * database families.
 */
class DatabaseFamilyInfo {
public:
    std::string  name;                  /// The name of a database family
    size_t       replicationLevel = 0;  /// The minimum replication level
    unsigned int numStripes = 0;        /// The number of stripes (from the CSS partitioning configuration)
    unsigned int numSubStripes = 0;     /// The number of sub-stripes (from the CSS partitioning configuration)
    double       overlap = 0.;          /// The default overlap (radians) for tables that do not specify their own overlap

    std::shared_ptr<ChunkNumberValidator> chunkNumberValidator;     /// A validator for chunk numbers

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, DatabaseFamilyInfo const& info);


/**
  * Class ConfigurationIFace is an interface for a family of concrete classes
  * providing configuration services for the components of the Replication
  * system.
  */
class ConfigurationIFace {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<ConfigurationIFace> Ptr;

    // Copy semantics is prohibited

    ConfigurationIFace(ConfigurationIFace const&) = delete;
    ConfigurationIFace& operator=(ConfigurationIFace const&) = delete;

    virtual ~ConfigurationIFace() = default;

    /// @return The configuration prefix.
    virtual std::string prefix() const = 0;

    /**
     * Construct the original (minus security-related info) path to
     * the configuration source.
     *
     * @oparam showPassword If a value of the flag is 'false' then hash a password
     *   in the result.
     * @return The constructed path.
     */
    virtual std::string configUrl(bool showPassword=false) const = 0;

    /**
     * Return the names of known workers as per the selection criteria.
     * 
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
    virtual std::vector<std::string> workers(bool isEnabled=true,
                                             bool isReadOnly=false) const = 0;

    /// @return The names of all known workers regardless of their statuses.
    virtual std::vector<std::string> allWorkers() const = 0;

    /// @return A maximum size of the request buffers in bytes.
    virtual size_t requestBufferSizeBytes() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setRequestBufferSizeBytes(size_t val,
                                           bool updatePersistentState=true) = 0;

    /// @return A timeout in seconds for the network retry operations.
    virtual unsigned int retryTimeoutSec() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setRetryTimeoutSec(unsigned int val,
                                    bool updatePersistentState=true) = 0;

    /// @return A number of threads to launch for BOOST ASIO within the controller.
    virtual size_t controllerThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setControllerThreads(size_t val,
                                      bool updatePersistentState=true) = 0;

    /// @return A port number for the controller's HTTP server.
    virtual uint16_t controllerHttpPort() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setControllerHttpPort(uint16_t val,
                                       bool updatePersistentState=true) = 0;

    /// @return The number of threads to run within the controller's HTTP server.
    virtual size_t controllerHttpThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setControllerHttpThreads(size_t val,
                                          bool updatePersistentState=true) = 0;


    /// @return An expiration timeout for requests.
    virtual unsigned int controllerRequestTimeoutSec() const = 0;

    /// @return A path to a folder where Qserv master stores its "empty chunk lists".
    virtual std::string controllerEmptyChunksDir() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setControllerRequestTimeoutSec(unsigned int val,
                                                bool updatePersistentState=true) = 0;

    /// @return An expiration timeout for jobs.
    virtual unsigned int jobTimeoutSec() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setJobTimeoutSec(unsigned int val,
                                  bool updatePersistentState=true) = 0;

    /// @return A timeout in seconds for the job's heartbeats.
    virtual unsigned int jobHeartbeatTimeoutSec() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setJobHeartbeatTimeoutSec(unsigned int val,
                                           bool updatePersistentState=true) = 0;

    /// @return A flag indicating if Qserv should be automatically notified on changes.
    virtual bool xrootdAutoNotify() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setXrootdAutoNotify(bool val,
                                     bool updatePersistentState=true) = 0;

    /// @return The host name of the worker XRootD service.
    virtual std::string xrootdHost() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setXrootdHost(std::string const& val,
                               bool updatePersistentState=true) = 0;


    /// @return A port number of the worker XRootD service.
    virtual uint16_t xrootdPort() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setXrootdPort(uint16_t val,
                               bool updatePersistentState=true) = 0;

    // @return An expiration timeout for requests.
    virtual unsigned int xrootdTimeoutSec() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setXrootdTimeoutSec(unsigned int val,
                                     bool updatePersistentState=true) = 0;

    /// @return The name of a database technology for worker services.
    virtual std::string databaseTechnology() const = 0;

    /// @return The DNS name or IP address of a machine of a database service.
    virtual std::string databaseHost() const = 0;

    /// @return The port number of the database service.
    virtual uint16_t databasePort() const = 0;

    /// @return The name of a database user.
    virtual std::string databaseUser() const = 0;

    /// @return The database password.
    virtual std::string databasePassword() const = 0;

    /// @return The name of a database to be set upon the connection.
    virtual std::string databaseName() const = 0;

    /// @return The number of concurrent connections to the database service.
    virtual size_t databaseServicesPoolSize() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setDatabaseServicesPoolSize(size_t val,
                                             bool updatePersistentState=true) = 0;

    /// @return The DNS name or IP address of a machine of a database service.
    virtual std::string qservMasterDatabaseHost() const = 0;

    /// @return The port number of the database service.
    virtual uint16_t qservMasterDatabasePort() const = 0;

    /// @return The name of a database user.
    virtual std::string qservMasterDatabaseUser() const = 0;

    /// @return The name of a database to be set upon the connection.
    virtual std::string qservMasterDatabaseName() const = 0;

    /// @return The number of concurrent connections to the database service.
    virtual size_t qservMasterDatabaseServicesPoolSize() const = 0;

    /// @return A path for exchanging data with master's MySQL service.
    ///   in the 'LOAD DATA INFILE' and similar queries.
    virtual std::string qservMasterDatabaseTmpDir() const = 0;

    /// @return names of known database families
    virtual std::vector<std::string> databaseFamilies() const = 0;

    /**
     * @param name The name of a family
     * @return 'true' if the specified database family is known to the configuration.
     */
    virtual bool isKnownDatabaseFamily(std::string const& name) const = 0;

    /**
     * @param name The name of a family.
     * @return The database family description.
     * @throw std::invalid_argument If the specified family was not found in
     *   the configuration.
     */
    virtual DatabaseFamilyInfo databaseFamilyInfo(std::string const& name) const = 0;

    /**
     * Register a new database family.
     * 
     * @param info Parameters of the family.
     * @return A description of the newly created database family.
     * @throw std::invalid_argument If the specified family already exists, or
     *   if the input descriptor has incorrect parameters (empty name, 0 values
     *   of the numbers of stripes or sub-stripes, or 0 value of the replication level).
     */
    virtual DatabaseFamilyInfo addDatabaseFamily(DatabaseFamilyInfo const& info) = 0;

    /**
     * Delete an existing family.
     * 
     * @param name The name of a family.
     * @throw std::invalid_argument If the specified family was not found in
     *   the configuration, or an empty string passed as a value of the parameter.
     */
    virtual void deleteDatabaseFamily(std::string const& name) = 0;

    /**
     * @param family The name of a database family.
     * @return The minimum number of chunk replicas for a database family.
     * @throw std::invalid_argument If the specified family was not found in
     *   the configuration.
     */
    virtual size_t replicationLevel(std::string const& family) const = 0;

    /**
     * @param family The optional name of a database family.
     * @param allDatabases The optional flag which if set to 'true' will result
     *   in returning all known database entries regardless of their PUBLISHED
     *   status. Otherwise subset of databases as determined by the second flag
     *   'isPublished' will get returned.
     * @param isPublished The optional flag which is used if flag 'all' is set
     *   to 'false' to narrow a collection of databases returned by the method.
     * @return The names of known databases. A result of the method may be
     *   limited to a subset of databases belonging to the specified family.
     * @throw std::invalid_argument If the specified family was not found in
     *   the configuration.
     */
    virtual std::vector<std::string> databases(std::string const& family=std::string(),
                                               bool allDatabases=false,
                                               bool isPublished=true) const = 0;

    /**
     * @param name The name of a database.
     * @return 'true' if the specified database is known in the Configuration.
     */
    virtual bool isKnownDatabase(std::string const& name) const = 0;

    /**
     * @param name The name of a database.
     * @return A database descriptor.
     * @throw std::invalid_argument If the specified database was not found in
     *   the configuration.
     */
    virtual DatabaseInfo databaseInfo(std::string const& name) const = 0;

    /**
     * Register a new database. The database will be put into the UNPUBLISHED
     * state.
     * 
     * @param info A database descriptor of which only the name of the database
     *   and the name of its family will be considered. Other attributes (including
     *   its publishing state an tables) will be ignored.
     * @return A database descriptor of the newly created database.
     * @throw std::invalid_argument If the specified database already exists, or
     *   if the database family is not valid, or if either of those parameters
     *   are the empty strings.
     */
    virtual DatabaseInfo addDatabase(DatabaseInfo const& info) = 0;

    /**
     * Change database status into PUBLISHED.
     * 
     * @param name The name of a database.
     * @return An updated database descriptor.
     * @throw std::invalid_argument If the specified database was not found in
     *  the configuration
     * @throw std::logic_error If the specified database is already PUBLISHED.
     */
    virtual DatabaseInfo publishDatabase(std::string const& name) = 0;

    /**
     * Delete an existing database.
     * 
     * @param name The name of a database to be deleted
     * @throw std::invalid_argument If the specified database doesn't exist, or
     *   if an empty string is passed as a parameters of the method.
     */
    virtual void deleteDatabase(std::string const& name) = 0;

    /**
     * Register a new table with a database.
     * 
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
    virtual DatabaseInfo addTable(std::string const& database,
                                  std::string const& table,
                                  bool isPartitioned,
                                  std::list<SqlColDef> const& columns=std::list<SqlColDef>(),
                                  bool isDirectorTable=false,
                                  std::string const& directorTableKey="objectId",
                                  std::string const& chunkIdColName=lsst::qserv::CHUNK_COLUMN,
                                  std::string const& subChunkIdColName=lsst::qserv::SUB_CHUNK_COLUMN,
                                  std::string const& latitudeColName=std::string(),
                                  std::string const& longitudeColName=std::string()) = 0;

    /**
     * Delete an existing table.
     * 
     * @param database The name of an existing database hosting the table.
     * @param table The name of an existing table to be deleted.
     * @throw std::invalid_argument If the specified database doesn't exists, or
     *   if the table doesn't exist, or if either of those parameters are
     *   the empty strings.
     */
    virtual DatabaseInfo deleteTable(std::string const& database,
                                     std::string const& table) = 0;

    /**
     * @param name The name of a worker.
     * @return 'true' if the specified worker is known to the configuration.
     */
    virtual bool isKnownWorker(std::string const& name) const = 0;

    /**
     * @param name The name of a worker.
     * @return A worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo workerInfo(std::string const& name) const = 0;

    /**
     * Register a new worker in the Configuration.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param workerInfo The worker description.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual void addWorker(WorkerInfo const& workerInfo) = 0;

    /**
     * Completely remove the specified worker from the Configuration.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual void deleteWorker(std::string const& name) = 0;

    /**
     * Change the status of the worker node to 'disabled' or 'enabled'
     * depending on a value of the optional parameter 'disable'.
     * Note that disabled workers will be disallowed in any replication
     * activities.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param disable (optional) disable if 'true', enable otherwise.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo disableWorker(std::string const& name,
                                     bool disable=true,
                                     bool updatePersistentState=true) = 0;

    /**
     * Change the status of the worker node to 'read-only' or 'read-write'
     * depending on a value of the optional parameter 'readOnly'.
     * Note that read-only workers will be disallowed as replica destinations
     * in any replication activities.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param readOnly An flag (optional) turning the worker into the read-only
     *   mode if 'true', or into read-write mode otherwise.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerReadOnly(std::string const& name,
                                         bool readOnly=true,
                                         bool updatePersistentState=true) = 0;

    /**
     * Change the host name of the worker's service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param host The name of the worker's host.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerSvcHost(std::string const& name,
                                        std::string const& host,
                                        bool updatePersistentState=true) = 0;

    /**
     * Change the port number of the worker's service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param port The number of the worker's port.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerSvcPort(std::string const& name,
                                        uint16_t port,
                                        bool updatePersistentState=true) = 0;

    /**
     * Change the host name of the worker's file service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param host The name of the worker file service's host.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerFsHost(std::string const& name,
                                       std::string const& host,
                                       bool updatePersistentState=true) = 0;

    /**
     * Change the port number of the worker's file service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param port The number of the worker file service's port.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerFsPort(std::string const& name,
                                       uint16_t port,
                                       bool updatePersistentState=true) = 0;

    /**
     * Change the data directory of the worker.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param dataDir The new file system path.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerDataDir(std::string const& name,
                                        std::string const& dataDir,
                                        bool updatePersistentState=true) = 0;

    /**
     * Change the host name of the worker's database service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param host The name of a database host to be used by the worker host.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerDbHost(std::string const& name,
                                       std::string const& host,
                                       bool updatePersistentState=true) = 0;

    /**
     * Change the port number of the worker's database service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param port The number of the database service port to be used by the worker.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerDbPort(std::string const& name,
                                       uint16_t port,
                                       bool updatePersistentState=true) = 0;

    /**
     * Change the user account name of the worker's database service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param user The name of a database user for the worker.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerDbUser(std::string const& name,
                                       std::string const& user,
                                       bool updatePersistentState=true) = 0;

    /**
     * Change the host name of the worker's Ingest service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param host The name of the worker loading service's host.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerLoaderHost(std::string const& name,
                                           std::string const& host,
                                           bool updatePersistentState=true) = 0;

    /**
     * Change the port number of the worker's Ingest service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation
     * @param port The number of the worker loading service's port.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerLoaderPort(std::string const& name,
                                           uint16_t port,
                                           bool updatePersistentState=true) = 0;

    /**
     * Change the temporary directory of the worker's Ingest service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param tmpDir The new file system path.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerLoaderTmpDir(std::string const& name,
                                             std::string const& tmpDir,
                                             bool updatePersistentState=true) = 0;

    /**
     * Change the host name of the worker's Data Exporting service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param host The name of the worker data exporting service's host.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerExporterHost(std::string const& name,
                                             std::string const& host,
                                             bool updatePersistentState=true) = 0;

    /**
     * Change the port number of the worker's Data Exporting service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation
     * @param port The number of the worker data exporting service's port.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerExporterPort(std::string const& name,
                                             uint16_t port,
                                             bool updatePersistentState=true) = 0;

    /**
     * Change the temporary directory of the worker's Data Exporting service.
     *
     * @note This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     *
     * @param name The name of a worker affected by the operation.
     * @param tmpDir The new file system path.
     * @param updatePersistentState The flag which if set to 'true' will result
     *   in propagating the change to the persistent store.
     * @return An updated worker descriptor.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    virtual WorkerInfo setWorkerExporterTmpDir(std::string const& name,
                                               std::string const& tmpDir,
                                               bool updatePersistentState=true) = 0;

    /// @return The name of the default technology for implementing requests.
    virtual std::string workerTechnology() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setWorkerTechnology(std::string const& val,
                                     bool updatePersistentState=true) = 0;

    /// @return The number of request processing threads in each worker service.
    virtual size_t workerNumProcessingThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setWorkerNumProcessingThreads(size_t val,
                                               bool updatePersistentState=true) = 0;

    /// @return The number of request processing threads in each worker's file service.
    virtual size_t fsNumProcessingThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setFsNumProcessingThreads(size_t val,
                                           bool updatePersistentState=true) = 0;

    /// @return The buffer size for the file I/O operations.
    virtual size_t workerFsBufferSizeBytes() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setWorkerFsBufferSizeBytes(size_t val,
                                            bool updatePersistentState=true) = 0;

    /// @return The number of request processing threads in each worker's Ingest service.
    virtual size_t loaderNumProcessingThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setLoaderNumProcessingThreads(size_t val,
                                               bool updatePersistentState=true) = 0;

    /// @return The number of request processing threads in each worker's Data Exporting service.
    virtual size_t exporterNumProcessingThreads() const = 0;

    /// @param val The new value of the parameter.
    /// @param updatePersistentState The flag which if set to 'true' will result
    ///   in propagating the change to the persistent store.
    virtual void setExporterNumProcessingThreads(size_t val,
                                                 bool updatePersistentState=true) = 0;

    /**
     * Serialize the configuration parameters into a string.
     *
     * @return The string representation of the cached Configuration.
     */
    virtual std::string asString() const = 0;

    /// Serialize the configuration parameters into the Logger.
    virtual void dumpIntoLogger() const = 0;

protected:
    ConfigurationIFace() = default;

    /**
     * @param func The (optional) name of a method/function requested the context string.
     * @return The context string for debugging and diagnostic printouts.
     */
    std::string context(std::string const& func=std::string()) const;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONIFACE_H
