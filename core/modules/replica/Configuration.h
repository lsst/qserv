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
 * This header defines an abstract class Configuration and a number of
 * other relevant classes, which represent a public interface to
 * the Configuration service of the Replication System. Specific implementations
 * of the service's interface are found in separate headers and source files.
 */

// System headers
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "util/Mutex.h"

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
 * Structure WorkerInfo encapsulates various parameters describing a worker.
 */
struct WorkerInfo {

    /// The logical name of a worker
    std::string name;

    /// The worker is allowed to participate in the replication operations
    bool isEnabled = true;

    /// The worker can only server as a source of replicas. New replicas can't
    /// be placed on it.
    bool isReadOnly = false;

    /// The host name (or IP address) of the worker service
    std::string svcHost;

    /// The port number of the worker service
    uint16_t svcPort = 0;

    /// The host name (or IP address) of the file service for the worker
    std::string fsHost;

    /// The port number for the file service for the worker
    uint16_t fsPort = 0;

    /// An absolute path to the data directory under which the MySQL database
    /// folders are residing.
    std::string dataDir;

    /// The port number of the worker database service
    uint16_t dbPort = 0;

    /// The host name (or IP address) of the database service for the worker
    std::string dbHost;

    /// The name of a user account for connecting to the database service
    std::string dbUser;

    /**
     * Translate the structure into JSON
     *
     * @return
     *   JSON array
     */
    nlohmann::json toJson() const;
};

/// Overloaded operator for dumping objects of class WorkerInfo
std::ostream& operator <<(std::ostream& os, WorkerInfo const& info);

/**
 * Structure DatabaseInfo encapsulates various parameters describing databases.
 */
struct DatabaseInfo {

    /// The name of a database
    std::string name;

    /// The name of the database family
    std::string family;

    /// The names of the partitioned tables
    std::vector<std::string> partitionedTables;

    /// The list of fully replicated tables
    std::vector<std::string> regularTables;

    /**
     * Translate the structure into JSON
     *
     * @return
     *   JSON array
     */
    nlohmann::json toJson() const;
};

/// Overloaded operator for dumping objects of class DatabaseInfo
std::ostream& operator <<(std::ostream& os, DatabaseInfo const& info);

/**
 * Structure DatabaseFamilyInfo encapsulates various parameters describing
 * database families.
 */
struct DatabaseFamilyInfo {

    /// The name of a database family
    std::string name;

    /// The minimum replication level desired (1..N)
    size_t replicationLevel = 0;

    /// The number of stripes (from the CSS partitioning configuration)
    unsigned int numStripes = 0;

    /// The number of sub-stripes (from the CSS partitioning configuration)
    unsigned int numSubStripes = 0;

    /// A validator for chunk numbers
    std::shared_ptr<ChunkNumberValidator> chunkNumberValidator;

    /**
     * Translate the structure into JSON
     *
     * @return
     *   JSON array
     */
    nlohmann::json toJson() const;
};

/// Overloaded operator for dumping objects of class DatabaseFamilyInfo
std::ostream& operator <<(std::ostream& os, DatabaseFamilyInfo const& info);

/**
  * Class Configuration is a base class for a family of concrete classes
  * providing configuration services for the components of the Replication
  * system.
  */
class Configuration {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Configuration> Ptr;

    /**
     * The static factory method will instantiate an instance of a subclass
     * corresponding to a prefix of the configuration URL. The following
     * prefixes are supported:
     * @code
     *   file:<path>
     *   mysql://[user][:password]@[host][:port][/database]
     * @code
     *
     * @param configUrl
     *   the configuration source
     *
     * @throw std::invalid_argument
     *   if the URL has unsupported prefix or it couldn't be parsed
     *                            
     * @throw std::runtime_error
     *   if the input configuration is not consistent with expectations of
     *   the application
     */
    static Ptr load(std::string const& configUrl);

    /**
     * The static factory method will instantiate an instance of a subclass
     * loaded from a key-value map. This instance will correspond to the following
     * special prefix:
     *
     *   map:
     *
     * @param kvMap
     *   the configuration source
     *
     * @throw std::runtime_error
     *   if the input configuration is not consistent
     *   with expectations of the application
     */
    static Ptr load(std::map<std::string, std::string> const& kvMap);

    // Copy semantics is prohibited

    Configuration(Configuration const&) = delete;
    Configuration& operator=(Configuration const&) = delete;

    virtual ~Configuration() = default;

    /// @return the configuration prefix
    virtual std::string prefix() const = 0;

    /**
     * Construct the original (minus security-related info) path to
     * the configuration source.
     *
     * @return the constructed path
     */
    virtual std::string configUrl() const = 0;

    // ------------------------------------------------------------------------
    // -- Common configuration parameters of both the controller and workers --
    // ------------------------------------------------------------------------

    /**
     * @param isEnabled
     *   select workers which are allowed to participate in the
     *   replication operations.
     *
     * @param isReadOnly
     *   a subclass of the 'enabled' workers which can only serve as
     *   a source of replicas. No replica modification (creation or
     *   deletion) operations would be allowed against those workers.
     *   NOTE: this filter only matters for the 'enabled' workers.
     *
     * @return
     *   the names of known workers which have the specified properties
     *   as per input filters.
     */
    std::vector<std::string> workers(bool isEnabled=true,
                                     bool isReadOnly=false) const;

    /// @return names of all known workers regardless of their statuses
    std::vector<std::string> allWorkers() const;

    /// @return maximum size of the request buffers in bytes
    size_t requestBufferSizeBytes() const { return _requestBufferSizeBytes; }

    /// @param val  the new value of the parameter
    virtual void setRequestBufferSizeBytes(size_t val) = 0;


    /// @return timeout in seconds for the network retry operations
    unsigned int retryTimeoutSec() const { return _retryTimeoutSec; }

    /// @param val  the new value of the parameter
    virtual void setRetryTimeoutSec(unsigned int val) = 0;


    // --------------------------------------------------------
    // -- Configuration parameters of the controller service --
    // --------------------------------------------------------


    /// @return number of threads to launch for BOOST ASIO within the controller
    size_t controllerThreads() const { return _controllerThreads; }

    /// @param val  the new value of the parameter
    virtual void setControllerThreads(size_t val) = 0;


    /// @return port number for the controller's HTTP server
    uint16_t controllerHttpPort() const { return _controllerHttpPort; }

    /// @param val  the new value of the parameter
    virtual void setControllerHttpPort(uint16_t val) = 0;


    /// @return number of threads to run within the controller's HTTP server
    size_t controllerHttpThreads() const { return _controllerHttpThreads; }

    /// @param val  the new value of the parameter
    virtual void setControllerHttpThreads(size_t val) = 0;


    // @return expiration timeout for requests
    unsigned int controllerRequestTimeoutSec() const { return _controllerRequestTimeoutSec; }

    /// @param val  the new value of the parameter
    virtual void setControllerRequestTimeoutSec(unsigned int val) = 0;


    // @return expiration timeout for jobs
    unsigned int jobTimeoutSec() const { return _jobTimeoutSec; }

    /// @param val  the new value of the parameter
    virtual void setJobTimeoutSec(unsigned int val) = 0;


    /// @return timeout in seconds for the job's heartbeats
    unsigned int jobHeartbeatTimeoutSec() const { return _jobHeartbeatTimeoutSec; }

    /// @param val  the new value of the parameter
    virtual void setJobHeartbeatTimeoutSec(unsigned int val) = 0;


    // --------------------------------------------------------
    // -- Qserv Worker Management Services  (via XRootD/SSI) --
    // --------------------------------------------------------


    /// @return flag indicating if Qserv should be automatically notified on changes
    bool xrootdAutoNotify() const { return  _xrootdAutoNotify; }

    /// @param val  the new value of the parameter
    virtual void setXrootdAutoNotify(bool val) = 0;


    /// @return host name of the worker XRootD service
    std::string const& xrootdHost() const { return  _xrootdHost; }

    /// @param val  the new value of the parameter
    virtual void setXrootdHost(std::string const& val) = 0;


    /// @return port number of the worker XRootD service
    uint16_t xrootdPort() const { return _xrootdPort; }

    /// @param val  the new value of the parameter
    virtual void setXrootdPort(uint16_t val) = 0;


    // @return expiration timeout for requests
    unsigned int xrootdTimeoutSec() const { return _xrootdTimeoutSec; }

    /// @param val  the new value of the parameter
    virtual void setXrootdTimeoutSec(unsigned int val) = 0;


    // -----------------------------------------------------------
    // -- Configuration parameters related to database services --
    // -----------------------------------------------------------

    /// @return the name of a database technology for worker services
    std::string const& databaseTechnology() const { return _databaseTechnology; }

    /// @return the DNS name or IP address of a machine of a database service
    std::string const& databaseHost() const { return _databaseHost; }

    /// @return the port number of the database service
    uint16_t databasePort() const { return _databasePort; }

    /// @return the name of a database user
    std::string const& databaseUser() const { return _databaseUser; }

    /// @return the database password
    std::string const& databasePassword() const { return _databasePassword; }

    /// @return the name of a database to be set upon the connection
    std::string const& databaseName() const { return _databaseName; }


    /// @return the number of concurrent connections to the database service
    size_t databaseServicesPoolSize() const { return _databaseServicesPoolSize; }

    /// @param val  the new value of the parameter
    virtual void setDatabaseServicesPoolSize(size_t val) = 0;


    // --------------------------------------------------
    // -- Global parameters of the database connectors --
    // --------------------------------------------------

    /**
     * @return the default mode for database reconnects.
      */
    static bool databaseAllowReconnect() { return defaultDatabaseAllowReconnect; }

    /**
     * Change the default value of a parameter defining a policy for handling
     * automatic reconnects to a database server. Setting 'true' will enable
     * reconnects.
     *
     * @param value
     *   new value of the parameter
     *
     * @return
     *   the previous value
     */
    static bool setDatabaseAllowReconnect(bool value);

    /**
     * @return the default timeout for connecting to database servers
     */
    static unsigned int databaseConnectTimeoutSec() { return defaultDatabaseConnectTimeoutSec; }

    /**
     * Change the default value of a parameter specifying delays between automatic
     * reconnects (should those be enabled by the corresponding policy).
     *
     * @param value
     *   new value of the parameter (must be strictly greater than 0)
     *
     * @return
     *   the previous value
     *
     * @throws std::invalid_argument
     *   if the new value of the parameter is 0
     */
    static unsigned int setDatabaseConnectTimeoutSec(unsigned int value);

    /**
     * @return the default number of a maximum number of attempts to execute
     * a query due to database connection failures and subsequent reconnects.
     */
    static unsigned int databaseMaxReconnects() { return defaultDatabaseMaxReconnects; }

    /**
     * Change the default value of a parameter specifying the maximum number
     * of attempts to execute a query due to database connection failures and
     * subsequent reconnects (should they be enabled by the corresponding policy).
     *
     * @param value
     *   new value of the parameter (must be strictly greater than 0)
     *
     * @return
     *   the previous value
     *
     * @throws std::invalid_argument
     *   if the new value of the parameter is 0
     */
    static unsigned int setDatabaseMaxReconnects(unsigned int value);

    /**
     * @return
     *   the default timeout for executing transactions at a presence
     *   of server reconnects.
     */
    static unsigned int databaseTransactionTimeoutSec() { return defaultDatabaseTransactionTimeoutSec; }

    /**
     * Change the default value of a parameter specifying a timeout for executing
     * transactions at a presence of server reconnects.
     *
     * @param value
     *   new value of the parameter (must be strictly greater than 0)
     *
     * @return
     *   the previous value
     *
     * @throws std::invalid_argument
     *   if the new value of the parameter is 0
     */
    static unsigned int setDatabaseTransactionTimeoutSec(unsigned int value);

    // ---------------------------------------------------
    // -- Configuration parameters related to databases --
    // ---------------------------------------------------

    /// @return names of known database families
    std::vector<std::string> databaseFamilies() const;

    /**
     * @param name
     *   the name of a family
     *
     * @return
     *    'true' if the specified database family is known to the configuration
     */
    bool isKnownDatabaseFamily(std::string const& name) const;

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
    DatabaseFamilyInfo databaseFamilyInfo(std::string const& name) const;

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
    size_t replicationLevel(std::string const& family) const;

    /**
     * @param family
     *   the optional name of a database family
     *
     * @return
     *   the names of known databases. A result of the method may be
     *   limited to a subset of databases belonging to the specified family.
    *
     * @throw std::invalid_argument
     *   if the specified family was not found in the configuration.
      */
    std::vector<std::string> databases(std::string const& family=std::string()) const;

    /**
     * @param name
     *   the name of a database
     *
     * @return
     *   'true' if the specified database is known in the configuration
     */
    bool isKnownDatabase(std::string const& name) const;

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
    DatabaseInfo databaseInfo(std::string const& name) const;

    /**
     * Register a new database
     * 
     * @param info
     *   database descriptor (only the name and the database family attributes
     *   will be considered.
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
     * @return
     *    a database descriptor of the updated database
     *
     * @throw std::invalid_argument
     *   if the specified database doesn't exists, or if the table already exists,
     *   or if either of those parameters are the empty strings
     */
    virtual DatabaseInfo addTable(std::string const& database,
                                  std::string const& table,
                                  bool isPartitioned) = 0;

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
    bool isKnownWorker(std::string const& name) const;

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
    WorkerInfo workerInfo(std::string const& name) const;

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

    /// @return the name of the default technology for implementing requests
    std::string const& workerTechnology() const { return _workerTechnology; }

    /// @param val  the new value of the parameter
    virtual void setWorkerTechnology(std::string const& val) = 0;


    /// @return the number of request processing threads in each worker service
    size_t workerNumProcessingThreads() const { return _workerNumProcessingThreads; }

    /// @param val  the new value of the parameter
    virtual void setWorkerNumProcessingThreads(size_t val) = 0;


    /// @return the number of request processing threads in each worker's file service
    size_t fsNumProcessingThreads() const { return _fsNumProcessingThreads; }

    /// @param val  the new value of the parameter
    virtual void setFsNumProcessingThreads(size_t val) = 0;


    /// @return the buffer size for the file I/O operations
    size_t workerFsBufferSizeBytes() const { return _workerFsBufferSizeBytes; }

    /// @param val  the new value of the parameter
    virtual void setWorkerFsBufferSizeBytes(size_t val) = 0;


    // -----------
    // -- Misc. --
    // -----------

    /**
     * Serialize the configuration parameters into a string
     *
     * @return
     *    string representation of the cached Configuration
     */
    std::string asString() const;

    /**
     * Serialize the configuration parameters into the Logger
     */
    void dumpIntoLogger() const;

protected:

    // Default values of some parameters are used by both the default constructor
    // of this class as well as by subclasses when initializing the configuration
    // object.

    static size_t       const defaultRequestBufferSizeBytes;
    static unsigned int const defaultRetryTimeoutSec;
    static size_t       const defaultControllerThreads;
    static uint16_t     const defaultControllerHttpPort;
    static size_t       const defaultControllerHttpThreads;
    static unsigned int const defaultControllerRequestTimeoutSec;
    static unsigned int const defaultJobTimeoutSec;
    static unsigned int const defaultJobHeartbeatTimeoutSec;
    static bool         const defaultXrootdAutoNotify;
    static std::string  const defaultXrootdHost;
    static uint16_t     const defaultXrootdPort;
    static unsigned int const defaultXrootdTimeoutSec;
    static std::string  const defaultWorkerTechnology;
    static size_t       const defaultWorkerNumProcessingThreads;
    static size_t       const defaultFsNumProcessingThreads;
    static size_t       const defaultWorkerFsBufferSizeBytes;
    static std::string  const defaultWorkerSvcHost;
    static uint16_t     const defaultWorkerSvcPort;
    static std::string  const defaultWorkerFsHost;
    static uint16_t     const defaultWorkerFsPort;
    static std::string  const defaultDataDir;
    static std::string  const defaultWorkerDbHost;
    static uint16_t     const defaultWorkerDbPort;
    static std::string  const defaultWorkerDbUser;
    static std::string  const defaultDatabaseTechnology;
    static std::string  const defaultDatabaseHost;
    static uint16_t     const defaultDatabasePort;
    static std::string  const defaultDatabaseUser;
    static std::string  const defaultDatabasePassword;
    static std::string  const defaultDatabaseName;
    static size_t       const defaultDatabaseServicesPoolSize;
    static bool               defaultDatabaseAllowReconnect;        // read-write
    static unsigned int       defaultDatabaseConnectTimeoutSec;     // read-write
    static unsigned int       defaultDatabaseMaxReconnects;         // read-write
    static unsigned int       defaultDatabaseTransactionTimeoutSec; // read-write
    static size_t       const defaultReplicationLevel;
    static unsigned int const defaultNumStripes;
    static unsigned int const defaultNumSubStripes;

    /**
     * In-place translation of the the data directory string by finding an optional
     * placeholder '{worker}' and replacing it with the name of the specified worker.
     *
     * @param dataDir
     *   the string to be translated
     * 
     * @param workerName
     *   the actual name of a worker for replacing the placeholder
     */
    static void translateDataDir(std::string&       dataDir,
                                 std::string const& workerName);
    /**
     * Construct the object
     *
     * The constructor will initialize the configuration parameters with
     * some default states, some of which are probably meaningless.
     */
    Configuration();

    /**
     * @param func
     *   (optional) the name of a method/function requested the context string
     *
     * @return
     *   the context string for debugging and diagnostic printouts
     */
    std::string context(std::string const& func=std::string()) const;

    /**
     * 
     * @param lock
     *   the lock on Configuration::_mtx required for the thread safety
     *
     * @param name
     *   the name of a worker to find
     *
     * @param context
     *   a context (usually - a class and a method) from which the operation was
     *   requested. This is used for error reporting if o such worker was found.
     *
     * @return
     *   an iterator pointing to the worker's position within a collection of workers
     *
     * @throw std::invalid_argument
     *   if the specified worker was not found in the configuration
     */
    std::map<std::string, WorkerInfo>::iterator safeFindWorker(util::Lock const& lock,
                                                               std::string const& name,
                                                               std::string const& context);


    /// To be used were thread safety is required
    mutable util::Mutex _mtx;

    // -- Cached values of parameters --

    size_t       _requestBufferSizeBytes;
    unsigned int _retryTimeoutSec;

    size_t       _controllerThreads;
    uint16_t     _controllerHttpPort;
    size_t       _controllerHttpThreads;
    unsigned int _controllerRequestTimeoutSec;
    unsigned int _jobTimeoutSec;
    unsigned int _jobHeartbeatTimeoutSec;

    // -- Qserv Worker Management Services  (via XRootD/SSI)

    bool         _xrootdAutoNotify;     ///< if set to 'true' then automatically notify Qserv
    std::string  _xrootdHost;           ///< host name of the worker XRootD service
    uint16_t     _xrootdPort;           ///< port number of the worker XRootD service
    unsigned int _xrootdTimeoutSec;     ///< expiration timeout for requests

    // -- Worker parameters --

    std::string  _workerTechnology;

    size_t _workerNumProcessingThreads;
    size_t _fsNumProcessingThreads;
    size_t _workerFsBufferSizeBytes;

    std::map<std::string, DatabaseFamilyInfo> _databaseFamilyInfo;
    std::map<std::string, DatabaseInfo>       _databaseInfo;
    std::map<std::string, WorkerInfo>         _workerInfo;

    // -- Database-specific parameters --

    std::string _databaseTechnology;

    /// The DNS name or IP address of a machine where the database
    /// server runs
    std::string _databaseHost;

    /// The port number of the database service
    uint16_t _databasePort;

    /// The name of a database user
    std::string _databaseUser;

    /// The database password
    std::string _databasePassword;

    /// The name of a database to be set upon the connection
    std::string _databaseName;

    /// @return the number of concurrent connections to the database service
    size_t _databaseServicesPoolSize;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATION_H
