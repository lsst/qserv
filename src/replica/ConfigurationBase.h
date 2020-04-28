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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONBASE_H
#define LSST_QSERV_REPLICA_CONFIGURATIONBASE_H

/**
 * This header defines an intermediate abstract base class ConfigurationBase class
 * for a family of specific implementations of the Configuration service's interface
 * which are found in separate headers and source files.
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
#include "replica/Common.h"
#include "replica/ConfigurationIFace.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ConfigurationBase is an intermediate abstract base class for
 * a family of the service's implementations.
 */
class ConfigurationBase : public ConfigurationIFace {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<ConfigurationBase> Ptr;

    /**
     * The static factory method will instantiate an instance of a subclass
     * corresponding to a prefix of the configuration URL. The following
     * prefixes are supported:
     * @code
     *   mysql://[user][:password]@[host][:port][/database]
     * @code
     *
     * @param configUrl The configuration source.
     * @throw std::invalid_argument If the URL has unsupported prefix or it couldn't
     *   be parsed.                         
     * @throw std::runtime_error If the input configuration is not consistent with
     *   expectations of the application.
     */
    static ConfigurationIFace::Ptr load(std::string const& configUrl);

    /**
     * The static factory method will instantiate an instance of a subclass
     * loaded from a key-value map. This instance will correspond to the following
     * special prefix:
     *
     *   map:
     *
     * @param kvMap The configuration source.
     * @throw std::runtime_error If the input configuration is not consistent
     *   with expectations of the application.
     */
    static ConfigurationIFace::Ptr load(std::map<std::string, std::string> const& kvMap);

    ConfigurationBase(ConfigurationBase const&) = delete;
    ConfigurationBase& operator=(ConfigurationBase const&) = delete;

    virtual ~ConfigurationBase() = default;

    std::vector<std::string> workers(bool isEnabled,
                                     bool isReadOnly) const final;

    std::vector<std::string> allWorkers() const final;

    size_t requestBufferSizeBytes() const final { return _requestBufferSizeBytes; }

    unsigned int retryTimeoutSec() const final { return _retryTimeoutSec; }

    size_t       controllerThreads()           const final { return _controllerThreads; }
    uint16_t     controllerHttpPort()          const final { return _controllerHttpPort; }
    size_t       controllerHttpThreads()       const final { return _controllerHttpThreads; }
    unsigned int controllerRequestTimeoutSec() const final { return _controllerRequestTimeoutSec; }
    std::string  controllerEmptyChunksDir()    const final { return _controllerEmptyChunksDir; }

    unsigned int jobTimeoutSec()          const final { return _jobTimeoutSec; }
    unsigned int jobHeartbeatTimeoutSec() const final { return _jobHeartbeatTimeoutSec; }

    bool         xrootdAutoNotify() const final { return  _xrootdAutoNotify; }
    std::string  xrootdHost()       const final { return  _xrootdHost; }
    uint16_t     xrootdPort()       const final { return _xrootdPort; }
    unsigned int xrootdTimeoutSec() const final { return _xrootdTimeoutSec; }

    std::string databaseTechnology()       const final { return _databaseTechnology; }
    std::string databaseHost()             const final { return _databaseHost; }
    uint16_t    databasePort()             const final { return _databasePort; }
    std::string databaseUser()             const final { return _databaseUser; }
    std::string databasePassword()         const final { return _databasePassword; }
    std::string databaseName()             const final { return _databaseName; }
    size_t      databaseServicesPoolSize() const final { return _databaseServicesPoolSize; }

    std::string qservMasterDatabaseHost() const final { return _qservMasterDatabaseHost; }
    uint16_t    qservMasterDatabasePort() const final { return _qservMasterDatabasePort; }
    std::string qservMasterDatabaseUser() const final { return _qservMasterDatabaseUser; }
    std::string qservMasterDatabaseName() const final { return _qservMasterDatabaseName; }
    size_t      qservMasterDatabaseServicesPoolSize() const final { return _qservMasterDatabaseServicesPoolSize; }
    std::string qservMasterDatabaseTmpDir() const final { return _qservMasterDatabaseTmpDir; }

    std::vector<std::string> databaseFamilies() const final;

    bool isKnownDatabaseFamily(std::string const& name) const final;

    DatabaseFamilyInfo databaseFamilyInfo(std::string const& name) const final;

    size_t replicationLevel(std::string const& family) const final;

    std::vector<std::string> databases(std::string const& family,
                                       bool allDatabases,
                                       bool isPublished) const final;

    bool isKnownDatabase(std::string const& name) const final;

    DatabaseInfo databaseInfo(std::string const& name) const final;

    bool isKnownWorker(std::string const& name) const final;

    WorkerInfo workerInfo(std::string const& name) const final;

    std::string workerTechnology() const final { return _workerTechnology; }

    size_t workerNumProcessingThreads() const final { return _workerNumProcessingThreads; }
    size_t fsNumProcessingThreads()     const final { return _fsNumProcessingThreads; }
    size_t workerFsBufferSizeBytes()    const final { return _workerFsBufferSizeBytes; }
    size_t loaderNumProcessingThreads() const final { return _loaderNumProcessingThreads; }
    size_t exporterNumProcessingThreads() const final { return _exporterNumProcessingThreads; }
    size_t httpLoaderNumProcessingThreads() const final { return _httpLoaderNumProcessingThreads; }

    std::string asString() const final;

    void dumpIntoLogger() const final;

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
    static std::string  const defaultControllerEmptyChunksDir;
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
    static size_t       const defaultLoaderNumProcessingThreads;
    static size_t       const defaultExporterNumProcessingThreads;
    static size_t       const defaultHttpLoaderNumProcessingThreads;
    static std::string  const defaultWorkerSvcHost;
    static uint16_t     const defaultWorkerSvcPort;
    static std::string  const defaultWorkerFsHost;
    static uint16_t     const defaultWorkerFsPort;
    static std::string  const defaultDataDir;
    static std::string  const defaultWorkerDbHost;
    static uint16_t     const defaultWorkerDbPort;
    static std::string  const defaultWorkerDbUser;
    static std::string  const defaultWorkerLoaderHost;
    static uint16_t     const defaultWorkerLoaderPort;
    static std::string  const defaultWorkerLoaderTmpDir;
    static std::string  const defaultWorkerExporterHost;
    static uint16_t     const defaultWorkerExporterPort;
    static std::string  const defaultWorkerExporterTmpDir;
    static std::string  const defaultWorkerHttpLoaderHost;
    static uint16_t     const defaultWorkerHttpLoaderPort;
    static std::string  const defaultWorkerHttpLoaderTmpDir;
    static std::string  const defaultDatabaseTechnology;
    static std::string  const defaultDatabaseHost;
    static uint16_t     const defaultDatabasePort;
    static std::string  const defaultDatabaseUser;
    static std::string  const defaultDatabasePassword;
    static std::string  const defaultDatabaseName;
    static size_t       const defaultDatabaseServicesPoolSize;
    static std::string  const defaultQservMasterDatabaseHost;
    static uint16_t     const defaultQservMasterDatabasePort;
    static std::string  const defaultQservMasterDatabaseUser;
    static std::string  const defaultQservMasterDatabaseName;
    static size_t       const defaultQservMasterDatabaseServicesPoolSize;
    static std::string  const defaultQservMasterDatabaseTmpDir;
    static size_t       const defaultReplicationLevel;
    static unsigned int const defaultNumStripes;
    static unsigned int const defaultNumSubStripes;

    /**
     * In-place translation of the a worker directory string by finding an optional
     * placeholder '{worker}' and replacing it with the name of the specified worker.
     *
     * @param path The string to be translated.
     * @param workerName The actual name of a worker for replacing the placeholder.
     */
    static void translateWorkerDir(std::string& path,
                                   std::string const& workerName);
    /**     *
     * The constructor will initialize the configuration parameters with
     * some default states, some of which are probably meaningless.
     */
    ConfigurationBase();

    /**
     * @param name The name of a worker to be found.
     * @param context A context (usually - a class and a method) from which
     *   the operation was requested. This is used for error reporting if no
     *   such worker was found.
     * @return An iterator pointing to the worker's position within a collection
     *   of workers.
     * @throw std::invalid_argument If the specified worker was not found in
     *   the configuration.
     */
    std::map<std::string, WorkerInfo>::iterator safeFindWorker(std::string const& name,
                                                               std::string const& context);

    /**
     * @param name The name of a database to be found.
     * @param context A context (usually - a class and a method) from which
     *   the operation was requested. This is used for error reporting if no such
     *   database was found.
     * @return An iterator pointing to the database's position within a collection
     *   of databases.
     * @throw std::invalid_argument If the specified database was not found in
     *   the configuration
     */
    std::map<std::string, DatabaseInfo>::iterator safeFindDatabase(std::string const& name,
                                                                   std::string const& context);

    /**
     * 
     * @param colName The name of a column to be found.
     * @param columns The schema definition.
     * @return 'true' if found.
     */
    bool columnInSchema(std::string const& colName,
                        std::list<SqlColDef> const& columns) const;

    /**
     * Validate table parameters and thrown exception std::invalid_argument
     * if any problems were found.
     *
     * @see ConfigurationBase::addTable()
     */
    void validateTableParameters(std::string const& context_,
                                 std::string const& database,
                                 std::string const& table,
                                 bool isPartitioned,
                                 std::list<SqlColDef> const& columns,
                                 bool isDirectorTable,
                                 std::string const& directorTableKey,
                                 std::string const& chunkIdColName,
                                 std::string const& subChunkIdColName,
                                 std::string const& latitudeColName,
                                 std::string const& longitudeColName) const;

    /**
     * Update the transient state of the database by adding a new table.
     *
     * @see ConfigurationBase::addTable()
     */
    DatabaseInfo addTableTransient(std::string const& context_,
                                   std::string const& database,
                                   std::string const& table,
                                   bool isPartitioned,
                                   std::list<SqlColDef> const& columns,
                                   bool isDirectorTable,
                                   std::string const& directorTableKey,
                                   std::string const& chunkIdColName,
                                   std::string const& subChunkIdColName,
                                   std::string const& latitudeColName,
                                   std::string const& longitudeColName);

    // -- Cached values of parameters --

    size_t       _requestBufferSizeBytes;
    unsigned int _retryTimeoutSec;

    size_t       _controllerThreads;
    uint16_t     _controllerHttpPort;
    size_t       _controllerHttpThreads;
    unsigned int _controllerRequestTimeoutSec;
    std::string  _controllerEmptyChunksDir;
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
    size_t _loaderNumProcessingThreads;
    size_t _exporterNumProcessingThreads;
    size_t _httpLoaderNumProcessingThreads;

    std::map<std::string, DatabaseFamilyInfo> _databaseFamilyInfo;
    std::map<std::string, DatabaseInfo>       _databaseInfo;
    std::map<std::string, WorkerInfo>         _workerInfo;

    // -- Database-specific parameters --

    std::string _databaseTechnology;
    std::string _databaseHost;
    uint16_t    _databasePort;
    std::string _databaseUser;
    std::string _databasePassword;
    std::string _databaseName;
    size_t      _databaseServicesPoolSize;

    std::string _qservMasterDatabaseTechnology;
    std::string _qservMasterDatabaseHost;
    uint16_t    _qservMasterDatabasePort;
    std::string _qservMasterDatabaseUser;
    std::string _qservMasterDatabaseName;
    size_t      _qservMasterDatabaseServicesPoolSize;
    std::string _qservMasterDatabaseTmpDir;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONBASE_H
