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
#include <iosfwd>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

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

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, WorkerInfo const& info);

/**
 * Class DatabaseInfo encapsulates various parameters describing databases.
 */
class DatabaseInfo {
public:

    std::string name;    /// The name of a database
    std::string family;  /// The name of the database family

    bool isPublished = false;   /// The status of the database

    std::vector<std::string> partitionedTables; /// The names of the partitioned tables
    std::vector<std::string> regularTables;     /// The list of fully replicated tables

    /// Table schema (optional) 
    std::map<std::string,                       // table name
             std::list<std::pair<std::string,   // column name
                       std::string>>> columns;  // column type

    /// @return the names of all tables
    std::vector<std::string> tables() const {
        std::vector<std::string> result = partitionedTables;
        result.insert(result.begin(), regularTables.begin(), regularTables.end());
        return result;
    }

    std::string directorTable;      /// The name of the Qserv "director" table if any
    std::string directorTableKey;   /// The name of the primary key column in the "director" table.

    // Names of special columns of the partitioned tables.

    std::string chunkIdColName;     // same for all partitioned tables
    std::string subChunkIdColName;  // same for all partitioned tables

    std::map<std::string,                   // table name
             std::string> latitudeColName;  // latitude (declination) column name

    std::map<std::string,                   // table name
             std::string> longitudeColName; // longitude (right ascension) column name

    /// @return table schema in format which is suitable for CSS
    /// @throws std::out_of_range if the table is unknown
    std::string schema4css(std::string const& table) const;

    /// @return JSON representation of the object
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

    /// @return the configuration prefix
    virtual std::string prefix() const = 0;

    /**
     * Construct the original (minus security-related info) path to
     * the configuration source.
     *
     * @oparam showPassword  if 'false' then hash a password in the result
     * @return the constructed path
     */
    virtual std::string configUrl(bool showPassword=false) const = 0;

    // ------------------------------------------------------------------------
    // -- Common configuration parameters of both the controller and workers --
    // ------------------------------------------------------------------------

    /**
     * Return the names of known workers as per the selection criteria.
     * 
     * @param isEnabled
     *   select workers which are allowed to participate in the
     *   replication operations. If a value of the parameter is set
     *   to 'true' then the next flag 'isReadOnly' (depending on its state)
     *   would put further restrictions on the selected subset.
     *   Workers which are not 'enabled' are still known to the Replication
     *   system.
     *
     * @param isReadOnly
     *   this flag will be considered only if 'isEnabled' is set to 'true'.
     *   The flag narrows down a subset of the 'enabled' workers which are
     *   either the read-only sources (if 'isReadOnly' is set to true')
     *   or the read-write replica sources/destinations.
     *   NOTE: no replica modification (creation or deletion) operations
     *   would be allowed against worker in the read-only state.
     *
     * @return
     *   the names of known workers which have the specified properties
     *   as per input filters.
     */
    virtual std::vector<std::string> workers(bool isEnabled=true,
                                             bool isReadOnly=false) const = 0;

    /// @return names of all known workers regardless of their statuses
    virtual std::vector<std::string> allWorkers() const = 0;

    /// @return maximum size of the request buffers in bytes
    virtual size_t requestBufferSizeBytes() const = 0;

    /// @param val  the new value of the parameter
    virtual void setRequestBufferSizeBytes(size_t val) = 0;

    /// @return timeout in seconds for the network retry operations
    virtual unsigned int retryTimeoutSec() const = 0;

    /// @param val  the new value of the parameter
    virtual void setRetryTimeoutSec(unsigned int val) = 0;

    // --------------------------------------------------------
    // -- Configuration parameters of the controller service --
    // --------------------------------------------------------

    /// @return number of threads to launch for BOOST ASIO within the controller
    virtual size_t controllerThreads() const = 0;

    /// @param val  the new value of the parameter
    virtual void setControllerThreads(size_t val) = 0;

    /// @return port number for the controller's HTTP server
    virtual uint16_t controllerHttpPort() const = 0;

    /// @param val  the new value of the parameter
    virtual void setControllerHttpPort(uint16_t val) = 0;

    /// @return number of threads to run within the controller's HTTP server
    virtual size_t controllerHttpThreads() const = 0;

    /// @param val  the new value of the parameter
    virtual void setControllerHttpThreads(size_t val) = 0;


    /// @return expiration timeout for requests
    virtual unsigned int controllerRequestTimeoutSec() const = 0;

    /// @return a path to a folder where Qserv master stores its "empty chunk lists"
    virtual std::string controllerEmptyChunksDir() const = 0;

    /// @param val  the new value of the parameter
    virtual void setControllerRequestTimeoutSec(unsigned int val) = 0;

    /// @return expiration timeout for jobs
    virtual unsigned int jobTimeoutSec() const = 0;

    /// @param val  the new value of the parameter
    virtual void setJobTimeoutSec(unsigned int val) = 0;

    /// @return timeout in seconds for the job's heartbeats
    virtual unsigned int jobHeartbeatTimeoutSec() const = 0;

    /// @param val  the new value of the parameter
    virtual void setJobHeartbeatTimeoutSec(unsigned int val) = 0;

    // --------------------------------------------------------
    // -- Qserv Worker Management Services  (via XRootD/SSI) --
    // --------------------------------------------------------

    /// @return flag indicating if Qserv should be automatically notified on changes
    virtual bool xrootdAutoNotify() const = 0;

    /// @param val  the new value of the parameter
    virtual void setXrootdAutoNotify(bool val) = 0;


    /// @return host name of the worker XRootD service
    virtual std::string xrootdHost() const = 0;

    /// @param val  the new value of the parameter
    virtual void setXrootdHost(std::string const& val) = 0;


    /// @return port number of the worker XRootD service
    virtual uint16_t xrootdPort() const = 0;

    /// @param val  the new value of the parameter
    virtual void setXrootdPort(uint16_t val) = 0;

    // @return expiration timeout for requests
    virtual unsigned int xrootdTimeoutSec() const = 0;

    /// @param val  the new value of the parameter
    virtual void setXrootdTimeoutSec(unsigned int val) = 0;

    // -----------------------------------------------------------
    // -- Configuration parameters related to database services --
    // -----------------------------------------------------------

    /// @return the name of a database technology for worker services
    virtual std::string databaseTechnology() const = 0;

    /// @return the DNS name or IP address of a machine of a database service
    virtual std::string databaseHost() const = 0;

    /// @return the port number of the database service
    virtual uint16_t databasePort() const = 0;

    /// @return the name of a database user
    virtual std::string databaseUser() const = 0;

    /// @return the database password
    virtual std::string databasePassword() const = 0;

    /// @return the name of a database to be set upon the connection
    virtual std::string databaseName() const = 0;

    /// @return the number of concurrent connections to the database service
    virtual size_t databaseServicesPoolSize() const = 0;

    /// @param val  the new value of the parameter
    virtual void setDatabaseServicesPoolSize(size_t val) = 0;

    // ------------------------------------------------------
    // -- Parameters of the Qserv master database services --
    // ------------------------------------------------------

    /// @return the DNS name or IP address of a machine of a database service
    virtual std::string qservMasterDatabaseHost() const = 0;

    /// @return the port number of the database service
    virtual uint16_t qservMasterDatabasePort() const = 0;

    /// @return the name of a database user
    virtual std::string qservMasterDatabaseUser() const = 0;

    /// @return the name of a database to be set upon the connection
    virtual std::string qservMasterDatabaseName() const = 0;

    /// @return the number of concurrent connections to the database service
    virtual size_t qservMasterDatabaseServicesPoolSize() const = 0;

    /// @return a path for exchanging data with master's MySQL service
    /// in the 'LOAD DATA INFILE' and similar queries.
    virtual std::string qservMasterDatabaseTmpDir() const = 0;

    // ---------------------------------------------------
    // -- Configuration parameters related to databases --
    // ---------------------------------------------------

    /// @return names of known database families
    virtual std::vector<std::string> databaseFamilies() const = 0;

    /**
     * @param name
     *   the name of a family
     *
     * @return
     *    'true' if the specified database family is known to the configuration
     */
    virtual bool isKnownDatabaseFamily(std::string const& name) const = 0;

    /**
     * @param name
     *   the name of a family
     *
     * @return
     *   database family description
     *
     * @throw std::invalid_argument
     *   if the specified family was not found in the configuration
     */
    virtual DatabaseFamilyInfo databaseFamilyInfo(std::string const& name) const = 0;

    /**
     * Register a new database family
     * 
     * @param info
     *   parameters of the family
     * 
     * @return
     *   a description of the newly created database family
     *
     * @throw std::invalid_argument
     *   if the specified family already exists, or if the input descriptor
     *   has incorrect parameters (empty name, 0 values of the numbers of stripes
     *   or sub-stripes, or 0 value of the replication level)
     */
    virtual DatabaseFamilyInfo addDatabaseFamily(DatabaseFamilyInfo const& info) = 0;

    /**
     * Delete an existing family
     * 
     * @param name
     *   the name of a family
     *
     * @throw std::invalid_argument
     *   if the specified family was not found in the configuration, or
     *   an empty string passed as a value of the parameter.
     */
    virtual void deleteDatabaseFamily(std::string const& name) = 0;

    /**
     * @param family
     *   the name of a database family
     *
     * @return
     *   the minimum number of chunk replicas for a database family
     *
     * @throw std::invalid_argument
     *   if the specified family was not found in the configuration.
     */
    virtual size_t replicationLevel(std::string const& family) const = 0;

    /**
     * @param family
     *   the optional name of a database family
     *
     * @param allDatabases
     *   the optional flag which if set to 'true' will result in returning all
     *   known database entries regardless of their PUBLISHED status. Otherwise
     *   subset of databases as determined by the second flag 'isPublished'
     *   will get returned.
     * 
     * @param isPublished
     *   the optional flag which is used if flag 'all' is set to 'false'
     *   to narrow a collection of databases returned by the method.
     *
     * @return
     *   the names of known databases. A result of the method may be
     *   limited to a subset of databases belonging to the specified family.
     *
     * @throw std::invalid_argument
     *   if the specified family was not found in the configuration.
     */
    virtual std::vector<std::string> databases(std::string const& family=std::string(),
                                               bool allDatabases=false,
                                               bool isPublished=true) const = 0;

    /**
     * @param name
     *   the name of a database
     *
     * @return
     *   'true' if the specified database is known in the configuration
     */
    virtual bool isKnownDatabase(std::string const& name) const = 0;

    /**
     * @param name
     *   the name of a database
     *
     * @return
     *   database descriptor
     *
     * @throw std::invalid_argument
     *   if the specified database was not found in the configuration
     */
    virtual DatabaseInfo databaseInfo(std::string const& name) const = 0;

    /**
     * Register a new database. The database will be put into the UNPUBLISHED
     * state.
     * 
     * @param info
     *   database descriptor of which only the name of the database and the name
     *   of its family will be considered. Other attributes (including its
     *   publishing state an tables) will be ignored.
     *
     * @return
     *    a database descriptor of the newly created database
     *
     * @throw std::invalid_argument
     *   if the specified database already exists, or if the database family is
     *   not valid, or if either of those parameters are the empty strings
     */
    virtual DatabaseInfo addDatabase(DatabaseInfo const& info) = 0;

    /**
     * Change database status into PUBLISHED
     * 
     * @param name
     *   the name of a database
     *
     * @return
     *   updated database descriptor
     *
     * @throw std::invalid_argument
     *   if the specified database was not found in the configuration
     *
     * @throw std::logic_error
     *   if the specified database is already PUBLISHED
     */
    virtual DatabaseInfo publishDatabase(std::string const& name) = 0;

    /**
     *  Delete an existing database
     * 
     * @param name
     *   the name of a database to be deleted
     *
     * @throw std::invalid_argument
     *   if the specified database doesn't exist, or if an empty string is
     *   passed as a parameters of the method
     */
    virtual void deleteDatabase(std::string const& name) = 0;

    /**
     * Register a new table with a database
     * 
     * @param database
     *   the name of an existing database hosting the new table
     *
     * @param table
     *   the name of a new table to be registered
     *
     * @param isPartitioned
     *   'true' if the table is partitioned
     *
     * @param columns
     *   (optional) column definitions as pairs of (name,type) of the table
     *
     * @param isDirectorTable
     *   (optional) flag indicating if this is the "director" table of
     *   the catalog. Note there could be only one such table in a catalog,
     *   and this table must be "partitioned".
     *
     * @param directorTableKey
     *   (optional) the name of a column in the "director" table which is
     *   used as an "object" identifier for astronomical objects. This parameter
     *   applies to the director tables only. And if provided (and allowed) the column
     *   must be found among the names of columns in a value of parameter "columns".
     * 
     * @param chunkIdColName
     *   (optional) the name of a column which stores identifiers of "chunks",
     *   This parameter applies to all "partitioned" tables, and if provided the column
     *   must be found among the names of columns in a value of parameter "columns".
     * 
     * @param subChunkIdColName
     *   (optional) the name of a column which stores identifiers of "sub-chunks",
     *   This parameter applies to all "partitioned" tables, and if provided the column
     *   must be found among the names of columns in a value of parameter "columns".
     *
     * @param latitudeColName
     *   (optional) the name of a column which stores the latitude
     *
     * @param longitudeColName
     *   (optional) the name of a column which stores the longitude
     *
     * @return
     *    a database descriptor of the updated database
     *
     * @throw std::invalid_argument
     *   if the specified database doesn't exists, or if the table already exists,
     *   or if either of those parameters are the empty strings, or other required
     *   parameters have incorrect values or missing.
     */
    virtual DatabaseInfo addTable(std::string const& database,
                                  std::string const& table,
                                  bool isPartitioned,
                                  std::list<std::pair<std::string,std::string>> const& columns=
                                        std::list<std::pair<std::string,std::string>>(),
                                  bool isDirectorTable=false,
                                  std::string const& directorTableKey="objectId",
                                  std::string const& chunkIdColName="chunkId",
                                  std::string const& subChunkIdColName="subChunkId",
                                  std::string const& latitudeColName=std::string(),
                                  std::string const& longitudeColName=std::string()) = 0;

    /**
     * Delete an existing table
     * 
     * @param database
     *   the name of an existing database hosting the table
     *
     * @param table
     *   the name of an existing table to be deleted
     *
     * @throw std::invalid_argument
     *   if the specified database doesn't exists, or if the table doesn't exist,
     *   or if either of those parameters are the empty strings
     */
    virtual DatabaseInfo deleteTable(std::string const& database,
                                     std::string const& table) = 0;

    // -----------------------------------------------------
    // -- Configuration parameters of the worker services --
    // -----------------------------------------------------

    /**
     * @param name
     *   the name of a worker
     *
     * @return
     *   'true' if the specified worker is known to the configuration
     */
    virtual bool isKnownWorker(std::string const& name) const = 0;

    /**
     * @param name
     *   the name of a worker
     *
     * @return
     *   worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo workerInfo(std::string const& name) const = 0;

    /**
     * Register a new worker in the Configuration.
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param workerInfo
     *   the worker description
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual void addWorker(WorkerInfo const& workerInfo) = 0;

    /**
     * Completely remove the specified worker from the Configuration.
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual void deleteWorker(std::string const& name) = 0;

    /**
     * Change the status of the worker node to 'disabled' or 'enabled'
     * depending on a value of the optional parameter 'disable'.
     * Note that disabled workers will be disallowed in any replication
     * activities.
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param disable
     *   (optional) disable if 'true', enable otherwise
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo disableWorker(std::string const& name,
                                     bool disable=true) = 0;

    /**
     * Change the status of the worker node to 'read-only' or 'read-write'
     * depending on a value of the optional parameter 'readOnly'.
     * Note that read-only workers will be disallowed as replica destinations
     * in any replication activities.
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param readOnly
     *   (optional) turn into the read-only mode if 'true', or into read-write
     *   mode otherwise
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerReadOnly(std::string const& name,
                                         bool readOnly=true) = 0;

    /**
     * Change the host name of the worker's service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param host
     *   the name of a host
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerSvcHost(std::string const& name,
                                        std::string const& host) = 0;

    /**
     * Change the port number of the worker's service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param port
     *   the number of a port
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerSvcPort(std::string const& name,
                                        uint16_t port) = 0;

    /**
     * Change the host name of the worker's file service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param host
     *   the name of a host
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerFsHost(std::string const& name,
                                       std::string const& host) = 0;

    /**
     * Change the port number of the worker's file service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param port
     *   the number of a port
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerFsPort(std::string const& name,
                                       uint16_t port) = 0;

    /**
     * Change the data directory of the worker
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param dataDir
     *   the new file system path
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerDataDir(std::string const& name,
                                        std::string const& dataDir) = 0;

    /**
     * Change the host name of the worker's database service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker affected by the operation
     *
     * @param host
     *   the name of a new host
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerDbHost(std::string const& name,
                                       std::string const& host) = 0;

    /**
     * Change the port number of the worker's database service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker affected by the operation
     *
     * @param port
     *   the number of a new port
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerDbPort(std::string const& name,
                                       uint16_t port) = 0;

    /**
     * Change the user account name of the worker's database service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker affected by the operation
     *
     * @param user
     *   the name of a new user
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerDbUser(std::string const& name,
                                       std::string const& user) = 0;

    /**
     * Change the host name of the worker's Ingest service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker affected by the operation
     *
     * @param host
     *   the name of a new host
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerLoaderHost(std::string const& name,
                                           std::string const& host) = 0;

    /**
     * Change the port number of the worker's Ingest service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker affected by the operation
     *
     * @param port
     *   the number of a new port
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerLoaderPort(std::string const& name,
                                           uint16_t port) = 0;

    /**
     * Change the temporary directory of the worker's Ingest service
     *
     * @note
     *   This operation may throw implementation-specific exceptions
     *   which are not covered by this technology-neutral interface.
     * @note
     *
     * @param name
     *   the name of a worker
     *
     * @param tmpDir
     *   the new file system path
     *
     * @return
     *   updated worker descriptor
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration.
     */
    virtual WorkerInfo setWorkerLoaderTmpDir(std::string const& name,
                                             std::string const& tmpDir) = 0;

    /// @return the name of the default technology for implementing requests
    virtual std::string workerTechnology() const = 0;

    /// @param val  the new value of the parameter
    virtual void setWorkerTechnology(std::string const& val) = 0;

    /// @return the number of request processing threads in each worker service
    virtual size_t workerNumProcessingThreads() const = 0;

    /// @param val  the new value of the parameter
    virtual void setWorkerNumProcessingThreads(size_t val) = 0;

    /// @return the number of request processing threads in each worker's file service
    virtual size_t fsNumProcessingThreads() const = 0;

    /// @param val  the new value of the parameter
    virtual void setFsNumProcessingThreads(size_t val) = 0;

    /// @return the buffer size for the file I/O operations
    virtual size_t workerFsBufferSizeBytes() const = 0;

    /// @param val  the new value of the parameter
    virtual void setWorkerFsBufferSizeBytes(size_t val) = 0;

    /// @return the number of request processing threads in each worker's Ingest service
    virtual size_t loaderNumProcessingThreads() const = 0;

    /// @param val  the new value of the parameter
    virtual void setLoaderNumProcessingThreads(size_t val) = 0;

    // -----------
    // -- Misc. --
    // -----------

    /**
     * Serialize the configuration parameters into a string
     *
     * @return
     *    string representation of the cached Configuration
     */
    virtual std::string asString() const = 0;

    /**
     * Serialize the configuration parameters into the Logger
     */
    virtual void dumpIntoLogger() const = 0;

protected:

    ConfigurationIFace() = default;

    /**
     * @param func
     *   (optional) the name of a method/function requested the context string
     *
     * @return
     *   the context string for debugging and diagnostic printouts
     */
    std::string context(std::string const& func=std::string()) const;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONIFACE_H
