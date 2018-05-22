/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// Configuration.h declares:
///
/// class Configuration
/// (see individual class documentation for more information)

// System headers
#include <map>
#include <memory>
#include <iosfwd>
#include <string>
#include <vector>

// Qserv headers
#include "util/Mutex.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/// The descriptor of a worker
struct WorkerInfo {

    /// The logical name of a worker
    std::string name;

    /// The worker is allowed to participate in the replication operations
    bool isEnabled;

    /// The worker can only server as a source of replicas. New replicas can't
    /// be placed on it.
    bool isReadOnly;

    /// The host name (or IP address) of the worker service
    std::string svcHost;

    /// The port number of the worker service
    uint16_t svcPort;

    /// The host name (or IP address) of the file service for the worker
    std::string fsHost;

    /// The port number for the file service for the worker
    uint16_t fsPort;

    /// An absolute path to the data directory under which the MySQL database
    /// folders are residing.
    std::string dataDir;
};

/// Overloaded operator for dumping objects of class WorkerInfo
std::ostream& operator <<(std::ostream& os, WorkerInfo const& info);

/// The descriptor of a database
struct DatabaseInfo {

    /// The name of a database
    std::string name;

    /// The name of the database family
    std::string family;

    /// The names of the partitioned tables
    std::vector<std::string> partitionedTables;

    /// The list of fully replicated tables
    std::vector<std::string> regularTables;
};

/// Overloaded operator for dumping objects of class DatabaseInfo
std::ostream& operator <<(std::ostream& os, DatabaseInfo const& info);

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
     *
     *   file:<path>
     *   mysql://[user][:password]@[host][:port][/database]
     *
     * @param configUrl - the configuration source
     *
     * @throw std::invalid_argument - if the URL has unsupported prefix or it
     *                                couldn't be parsed
     */
    static Ptr load(std::string const& configUrl);

    // Copy semantics is prohibited

    Configuration(Configuration const&) = delete;
    Configuration& operator=(Configuration const&) = delete;

    /// Destructor
    virtual ~Configuration() = default;

    /**
     * Return the original (minus security-related info) path to the configuration
     * source.
     */
    virtual std::string configUrl() const=0;

    // ------------------------------------------------------------------------
    // -- Common configuration parameters of both the controller and workers --
    // ------------------------------------------------------------------------

    /**
     * The names of known workers which have the specified properties
     * as per input filters.
     *
     * @param isEnabled  - select workers which are allowed to participate in the
     *                     replication operations.
     * @param isReadOnly - a subclass of the 'enabled' workers which can only serve as
     *                     a source of replicas. No replica modification (creation or
     *                     deletion) operations would be allowed against those workers.
     *                     NOTE: this filter only matters for the 'enabled' workers.
     */
    std::vector<std::string> workers(bool isEnabled=true,
                                     bool isReadOnly=false) const;

    /// @return maximum size of the request buffers in bytes
    size_t requestBufferSizeBytes() const { return _requestBufferSizeBytes; }

    /// @return timeout in seconds for the network retry operations
    unsigned int retryTimeoutSec() const { return _retryTimeoutSec; }

    // --------------------------------------------------------
    // -- Configuration parameters of the controller service --
    // --------------------------------------------------------

    /// @return number of threads to launch for BOOST ASIO within the controller
    size_t controllerThreads() const { return _controllerThreads; }

    /// @return port number for the controller's HTTP server
    uint16_t controllerHttpPort() const { return _controllerHttpPort; }

    /// @return number of threads to run within the controller's HTTP server
    size_t controllerHttpThreads() const { return _controllerHttpThreads; }

    // @return expiration timeout for requests
    unsigned int controllerRequestTimeoutSec() const { return _controllerRequestTimeoutSec; }

    // @return expiration timeout for jobs
    unsigned int jobTimeoutSec() const { return _jobTimeoutSec; }

    /// @return timeout in seconds for the jobs' heartbeats
    unsigned int jobHeartbeatTimeoutSec() const { return _jobHeartbeatTimeoutSec; }

    // --------------------------------------------------------
    // -- Qserv Worker Management Services  (via XRootD/SSI) --
    // --------------------------------------------------------

    /// @return flag indicating if Qserv should be automatically notified on changes
    bool xrootdAutoNotify() const { return  _xrootdAutoNotify; }

    /// @return host name of the worker XRootD service
    std::string const& xrootdHost() const { return  _xrootdHost; }

    /// @return port number of the worker XRootD service
    uint16_t xrootdPort() const { return _xrootdPort; }

    // @return expiration timeout for requests
    unsigned int xrootdTimeoutSec() const { return _xrootdTimeoutSec; }

    // -----------------------------------------------------------
    // -- Configuration parameters related to database services --
    // -----------------------------------------------------------

    std::string const& databaseTechnology() const { return _databaseTechnology; }

    /// The DNS name or IP address of a machine where the database
    /// server runs
    std::string const& databaseHost() const { return _databaseHost; }

    /// The port number of the database service
    uint16_t databasePort() const { return _databasePort; }

    /// The name of a database user
    std::string const& databaseUser() const { return _databaseUser; }

    /// The database password
    std::string const& databasePassword() const { return _databasePassword; }

    /// The name of a database to be set upon the connection
    std::string const& databaseName() const { return _databaseName; }

    // ---------------------------------------------------
    // -- Configuration parameters related to databases --
    // ---------------------------------------------------

    /// The names of known database families
    std::vector<std::string> databaseFamilies() const;

    /**
     * Return 'true' if the specified database family is known to the configuraion
     *
     * @param name - the name of a family
     */
    bool isKnownDatabaseFamily(std::string const& name) const;

    /**
     * Return the minimum number of chunk replicas for a database family
     *
     * @param family - the name of a database family
     *
     * @throw std::invalid_argument - if the specified family was not found in
     *                                the configuration.
     */
    size_t replicationLevel(std::string const& family) const;

    /**
     * Return the names of known databases. A result of the method may be
     * limited to a subset of databases belonging ot the specified family.
     *
     * @param family - the optional name of a database family
     *
     * @throw std::invalid_argument - if the specified family was not found in
     *                                the configuration.
     */
    std::vector<std::string> databases(std::string const& family=std::string()) const;

    /**
     * Return 'true' if the specified database is known in the configuraion
     *
     * @param name - the name of a database
     */
    bool isKnownDatabase(std::string const& name) const;

    /**
     * Return parameters of the specified database
     *
     * @param name - the name of a database
     *
     * @throw std::invalid_argument - if the specified database was not found in
     *                                the configuration
     */
    DatabaseInfo const& databaseInfo(std::string const& name) const;

    // -----------------------------------------------------
    // -- Configuration parameters of the worker services --
    // -----------------------------------------------------

    /**
     * Return 'true' if the specified worker is known to the configuraion
     *
     * @param name - the name of a worker
     */
    bool isKnownWorker(std::string const& name) const;

    /**
     * Return parameters of the specified worker
     *
     * @param name - the name of a worker
     *
     * @throw std::invalid_argument - if the specified worker was not found in
     *                                the configuration.
     */
    WorkerInfo const& workerInfo(std::string const& name) const;

    /**
     * Change the status of the worker node to 'disabled' thus disallowing
     * its use for any replication activities. Return the updated descriptor
     * of the worker service. Note that if the operation fails to update
     * the configuration then it won't throw any exceptions. In that case it will
     * just a post a complain into the corresponding log stream. It's up
     * to caller of this method to check the new status of the worker in
     * the returned descriptor.:
     * @code
     *   try {
     *       if (config.disableWorker("worker-name").is_enabled) {
     *           std::cerr << "failed to disable the worker" << std::endl;
     *       }
     *   } catch (std::invalid_argument const& ex) {
     *       std::cerr << "the worker is unknown" << std::endl;
     *   }
     * @code
     *
     * @param name - the name of a worker
     *
     * @return the updated status of the worker
     *
     * @throw std::invalid_argument - if the specified worker was not found in
     *                                the configuration.
     */
    virtual WorkerInfo const& disableWorker(std::string const& name)=0;

    /**
     * Completelly remove the specified worker from the Configuration.
     *
     * @param name - the name of a worker
     *
     * @throw std::invalid_argument - if the specified worker was not found in
     *                                the configuration.
     */
    virtual void deleteWorker(std::string const& name)=0;

    /// Return the name of the default technology for implementing requests
    std::string const& workerTechnology() const { return _workerTechnology; }

    /// The number of request processing threads in each worker service
    size_t workerNumProcessingThreads() const { return _workerNumProcessingThreads; }

    /// The number of request processing threads in each worker's file service
    size_t fsNumProcessingThreads() const { return _fsNumProcessingThreads; }

    /// Return the buffer size for the file I/O operations
    size_t workerFsBufferSizeBytes() const { return _workerFsBufferSizeBytes; }

    // ---------------------------------------------------
    // -- Configuration parameters of the Job Scheduler --
    // ---------------------------------------------------

    /// Return the number of seconds betwean re-evaluations of the Schedule's
    /// state. At each expiration moment of the interval the Scheduler would
    /// check if there are new jobs which are requested to be run on the time basis.
    unsigned int jobSchedulerIvalSec() const { return _jobSchedulerIvalSec; }

    /**
     * Serialize the configuration parameters into the Logger
     */
    void dumpIntoLogger();

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
    static std::string  const defaultDatabaseTechnology;
    static std::string  const defaultDatabaseHost;
    static uint16_t     const defaultDatabasePort;
    static std::string  const defaultDatabaseUser;
    static std::string  const defaultDatabasePassword;
    static std::string  const defaultDatabaseName;
    static unsigned int const defaultJobSchedulerIvalSec;
    static size_t       const defaultReplicationLevel;

    /**
     * In-place translation of the the data directory string by finding an optional
     * placeholder '{worker}' and replacing it with the name of the specified worker.
     *
     * @param dataDir    - the string to be translated
     * @param workerName - the actual name of a worker for replacing the placeholder
     */
    static void translateDataDir(std::string&       dataDir,
                                 std::string const& workerName);
    /**
     * Construct the object
     *
     * The constructor will initialize the configuration parameters with
     * some default states, some of which are probably meaninless.
     */
    Configuration();

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

protected:

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

    /// The minimum number of replicas for members of each database family
    /// Allowed values: 1..N
    std::map<std::string, size_t> _replicationLevel;

    std::map<std::string, DatabaseInfo> _databaseInfo;
    std::map<std::string, WorkerInfo>   _workerInfo;

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

    // -- Parameters of the Job scheduler --

    unsigned int _jobSchedulerIvalSec;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATION_H
