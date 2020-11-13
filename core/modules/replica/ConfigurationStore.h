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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H
#define LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H

/**
 * This header defines a class which is used in an implementation
 * of the Configuration service.
 */

// System headers
#include <cstdint>
#include <stdexcept>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/ConfigurationBase.h"

// LSST headers
#include "lsst/log/Log.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace util {
    class ConfigStore;
}}} // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationStore is a base class for a family of configuration
  * classes which are designed to load configuration parameters from a transient
  * configuration store. 
  *
  * This class also:
  *
  *   - enforces a specific schema for key names found in the store
  *   - ensures all required parameters are found in the input store
  *   - sets default values for the optional parameters
  *   - caches parameters in memory
  */
class ConfigurationStore : public ConfigurationBase {
public:
    ConfigurationStore() = delete;
    ConfigurationStore(ConfigurationStore const&) = delete;
    ConfigurationStore& operator=(ConfigurationStore const&) = delete;

    ~ConfigurationStore() override = default;

    void setRequestBufferSizeBytes(size_t val,
                                   bool updatePersistentState) final { _set(_requestBufferSizeBytes, val); }

    void setRetryTimeoutSec(unsigned int val,
                            bool updatePersistentState) final { _set(_retryTimeoutSec, val); }

    void setControllerThreads(size_t val,
                              bool updatePersistentState) final { _set(_controllerThreads, val); }

    void setControllerHttpPort(uint16_t val,
                               bool updatePersistentState) final { _set(_controllerHttpPort, val); }

    void setControllerHttpThreads(size_t val,
                                  bool updatePersistentState) final { _set(_controllerHttpThreads, val); }

    void setControllerRequestTimeoutSec(unsigned int val,
                                        bool updatePersistentState) final { _set(_controllerRequestTimeoutSec, val); }

    void setJobTimeoutSec(unsigned int val,
                          bool updatePersistentState) final { _set(_jobTimeoutSec, val); }

    void setJobHeartbeatTimeoutSec(unsigned int val,
                                   bool updatePersistentState) final { _set(_jobHeartbeatTimeoutSec, val, true); }

    void setXrootdAutoNotify(bool val,
                             bool updatePersistentState) final { _set(_xrootdAutoNotify, val); }

    void setXrootdHost(std::string const& val,
                       bool updatePersistentState) final { _set(_xrootdHost, val); }

    void setXrootdPort(uint16_t val,
                       bool updatePersistentState) final { _set(_xrootdPort, val); }

    void setXrootdTimeoutSec(unsigned int val,
                             bool updatePersistentState) final { _set(_xrootdTimeoutSec, val); }

    void setDatabaseServicesPoolSize(size_t val,
                                     bool updatePersistentState) final { _set(_databaseServicesPoolSize, val); }

    void addWorker(WorkerInfo const& workerInfo) final;

    void deleteWorker(std::string const& name) final;

    WorkerInfo disableWorker(std::string const& name,
                             bool disable,
                             bool updatePersistentState) final;

    WorkerInfo setWorkerReadOnly(std::string const& name,
                                 bool readOnly,
                                 bool updatePersistentState) final;

    WorkerInfo setWorkerSvcHost(std::string const& name,
                                std::string const& host,
                                bool updatePersistentState) final;

    WorkerInfo setWorkerSvcPort(std::string const& name,
                                uint16_t port,
                                bool updatePersistentState) final;

    WorkerInfo setWorkerFsHost(std::string const& name,
                               std::string const& host,
                               bool updatePersistentState) final;

    WorkerInfo setWorkerFsPort(std::string const& name,
                               uint16_t por,
                               bool updatePersistentStatet) final;

    WorkerInfo setWorkerDataDir(std::string const& name,
                                std::string const& dataDir,
                                bool updatePersistentState) final;

    WorkerInfo setWorkerDbHost(std::string const& name,
                               std::string const& host,
                               bool updatePersistentState) final;

    WorkerInfo setWorkerDbPort(std::string const& name,
                               uint16_t port,
                               bool updatePersistentState) final;

    WorkerInfo setWorkerDbUser(std::string const& name,
                               std::string const& user,
                               bool updatePersistentState) final;

    WorkerInfo setWorkerLoaderHost(std::string const& name,
                                   std::string const& host,
                                   bool updatePersistentState) final;

    WorkerInfo setWorkerLoaderPort(std::string const& name,
                                   uint16_t port,
                                   bool updatePersistentState) final;

    WorkerInfo setWorkerLoaderTmpDir(std::string const& name,
                                     std::string const& tmpDir,
                                     bool updatePersistentState) final;

    WorkerInfo setWorkerExporterHost(std::string const& name,
                                     std::string const& host,
                                     bool updatePersistentState=true) final;

    WorkerInfo setWorkerExporterPort(std::string const& name,
                                     uint16_t port,
                                     bool updatePersistentState=true) final;

    WorkerInfo setWorkerExporterTmpDir(std::string const& name,
                                       std::string const& tmpDir,
                                       bool updatePersistentState=true) final;

    WorkerInfo setWorkerHttpLoaderHost(std::string const& name,
                                       std::string const& host,
                                       bool updatePersistentState=true) final;

    WorkerInfo setWorkerHttpLoaderPort(std::string const& name,
                                       uint16_t port,
                                       bool updatePersistentState=true) final;

    WorkerInfo setWorkerHttpLoaderTmpDir(std::string const& name,
                                         std::string const& tmpDir,
                                         bool updatePersistentState=true) final;

    void setWorkerTechnology(std::string const& val,
                             bool updatePersistentState) final { _set(_workerTechnology, val); }

    void setWorkerNumProcessingThreads(size_t val,
                                       bool updatePersistentState) final { _set(_workerNumProcessingThreads, val); }

    void setFsNumProcessingThreads(size_t val,
                                   bool updatePersistentState) final { _set(_fsNumProcessingThreads, val); }

    void setWorkerFsBufferSizeBytes(size_t val,
                                    bool updatePersistentState) final { _set(_workerFsBufferSizeBytes, val); }

    void setLoaderNumProcessingThreads(size_t val,
                                       bool updatePersistentState) final { _set(_loaderNumProcessingThreads, val); }

    void setExporterNumProcessingThreads(size_t val,
                                         bool updatePersistentState) final { _set(_exporterNumProcessingThreads, val); }

    void setHttpLoaderNumProcessingThreads(size_t val,
                                           bool updatePersistentState) final { _set(_httpLoaderNumProcessingThreads, val); }

    DatabaseFamilyInfo addDatabaseFamily(DatabaseFamilyInfo const& info) final;

    void deleteDatabaseFamily(std::string const& name) final;

    DatabaseInfo addDatabase(DatabaseInfo const& info) final;

    DatabaseInfo publishDatabase(std::string const& name) final;

    DatabaseInfo unPublishDatabase(std::string const& name) final;

    void deleteDatabase(std::string const& name) final;

    DatabaseInfo addTable(std::string const& database,
                          std::string const& table,
                          bool isPartitioned,
                          std::list<SqlColDef> const& columns,
                          bool isDirectorTable,
                          std::string const& directorTableKey,
                          std::string const& chunkIdColName,
                          std::string const& subChunkIdColName,
                          std::string const& latitudeColName,
                          std::string const& longitudeColName) final;

    DatabaseInfo deleteTable(std::string const& database,
                             std::string const& table) final;

protected:

    /**
     * Construct an object by reading the configuration from the input
     * configuration store.
     *
     * @param configStore
     *   reference to a configuration store object
     *
     * @throw std::runtime_error
     *   if the input configuration is not consistent with expectations
     *   of the application
     */
    explicit ConfigurationStore(util::ConfigStore const& configStore);

private:
    static std::string _classMethodContext(std::string const& func);

    /**
     * Read and validate input configuration parameters from the specified 
     * store and initialize the object.
     *
     * @param configStore A reference to a configuration store object.
     * @throw std::runtime_error If the input configuration is not consistent with
     *   expectations of the application.
     */
    void _loadConfiguration(util::ConfigStore const& configStore);

    /**
     * The setter method for numeric types.
     * 
     * @param var A reference to a parameter variable to be set.
     * @param val The new value of the parameter.
     * @param allowZero The (optional) flag disallowing (if set) zero values.
     */
    template <class T>
    void _set(T& var, T val, bool allowZero=false) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << val);
        if (not allowZero and val == 0) {
            throw std::invalid_argument(
                    "ConfigurationStore::" + std::string(__func__) + "<numeric>  0 value is not allowed");
        }
        var = val;
    }

    /**
     * Specialized version of the setter method for type 'bool'.
     */
    void _set(bool& var, bool val) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << (val ? "true" : "false"));
        var = val;
    }

    /**
     * Specialized version of the setter method for type 'std::string'.
     * 
     * @param var A reference to a parameter variable to be set.
     * @param val The new value of the parameter.
     * @param allowEmpty The (optional) flag disallowing (if set) empty values.
     */
    void _set(std::string& var, std::string const& val, bool allowEmpty=false) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << val);
        if (not allowEmpty and val.empty()) {
            throw std::invalid_argument(
                    "ConfigurationStore::" + std::string(__func__) + "<string>  empty value is not allowed");
        }
        var = val;
    }

    /// Message logger
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H
