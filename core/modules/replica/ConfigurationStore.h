/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include <stdexcept>
#include <string>

// Qserv headers
#include "replica/Configuration.h"

// LSST headers
#include "lsst/log/Log.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace util {
class ConfigStore;
}}} // namespace lsst::qserv::util

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
class ConfigurationStore
    :   public Configuration {

public:

    // Default construction and copy semantics are prohibited

    ConfigurationStore() = delete;
    ConfigurationStore(ConfigurationStore const&) = delete;
    ConfigurationStore& operator=(ConfigurationStore const&) = delete;

    ~ConfigurationStore() override = default;

    /**
     * @see Configuration::setRequestBufferSizeBytes()
     */
    void setRequestBufferSizeBytes(size_t val) final { _set(_requestBufferSizeBytes, val); }

    /**
     * @see Configuration::setRetryTimeoutSec()
     */
    void setRetryTimeoutSec(unsigned int val) final { _set(_retryTimeoutSec, val); }

    /**
     * @see Configuration::setControllerThreads()
     */
    void setControllerThreads(size_t val) final { _set(_controllerThreads, val); }

    /**
     * @see Configuration::setControllerHttpPort()
     */
    void setControllerHttpPort(uint16_t val) final { _set(_controllerHttpPort, val); }

    /**
     * @see Configuration::setControllerHttpThreads()
     */
    void setControllerHttpThreads(size_t val) final { _set(_controllerHttpThreads, val); }

    /**
     * @see Configuration::setControllerRequestTimeoutSec()
     */
    void setControllerRequestTimeoutSec(unsigned int val) final { _set(_controllerRequestTimeoutSec, val); }

    /**
     * @see Configuration::setJobTimeoutSec()
     */
    void setJobTimeoutSec(unsigned int val) final { _set(_jobTimeoutSec, val); }

    /**
     * @see Configuration::setJobHeartbeatTimeoutSec()
     */
    void setJobHeartbeatTimeoutSec(unsigned int val) final { _set(_jobHeartbeatTimeoutSec, val, true); }

    /**
     * @see Configuration::setXrootdAutoNotify()
     */
    void setXrootdAutoNotify(bool val) final { _set(_xrootdAutoNotify, val); }

    /**
     * @see Configuration::setXrootdHost()
     */
    void setXrootdHost(std::string const& val) final { _set(_xrootdHost, val); }

    /**
     * @see Configuration::setXrootdPort()
     */
    void setXrootdPort(uint16_t val) final { _set(_xrootdPort, val); }

    /**
     * @see Configuration::setXrootdTimeoutSec()
     */
    void setXrootdTimeoutSec(unsigned int val) final { _set(_xrootdTimeoutSec, val); }

    /**
     * @see Configuration::setDatabaseServicesPoolSize()
     */
    void setDatabaseServicesPoolSize(size_t val) final { _set(_databaseServicesPoolSize, val); }

    /**
     * @see Configuration::addWorker()
     */
    void addWorker(WorkerInfo const& workerInfo) final;

    /**
     * @see Configuration::deleteWorker()
     */
    void deleteWorker(std::string const& name) final;

    /**
     * @see Configuration::disableWorker()
     */
    WorkerInfo const disableWorker(std::string const& name,
                                   bool disable) final;

    /**
     * @see Configuration::setWorkerReadOnly()
     */
    WorkerInfo const setWorkerReadOnly(std::string const& name,
                                       bool readOnly) final;

    /**
     * @see Configuration::setWorkerSvcHost()
     */
    WorkerInfo const setWorkerSvcHost(std::string const& name,
                                      std::string const& host) final;

    /**
     * @see Configuration::setWorkerSvcPort()
     */
    WorkerInfo const setWorkerSvcPort(std::string const& name,
                                      uint16_t port) final;

    /**
     * @see Configuration::setWorkerFsHost()
     */
    WorkerInfo const setWorkerFsHost(std::string const& name,
                                     std::string const& host) final;

    /**
     * @see Configuration::setWorkerFsPort()
     */
    WorkerInfo const setWorkerFsPort(std::string const& name,
                                     uint16_t port) final;

    /**
     * @see Configuration::setWorkerDataDir()
     */
    WorkerInfo const setWorkerDataDir(std::string const& name,
                                      std::string const& dataDir) final;

    /**
     * @see Configuration::setWorkerTechnology()
     */
    void setWorkerTechnology(std::string const& val) final { _set(_workerTechnology, val); }

    /**
     * @see Configuration::setWorkerNumProcessingThreads()
     */
    void setWorkerNumProcessingThreads(size_t val) final { _set(_workerNumProcessingThreads, val); }

    /**
     * @see Configuration::setFsNumProcessingThreads()
     */
    void setFsNumProcessingThreads(size_t val) final { _set(_fsNumProcessingThreads, val); }

    /**
     * @see Configuration::setWorkerFsBufferSizeBytes()
     */
    void setWorkerFsBufferSizeBytes(size_t val) final { _set(_workerFsBufferSizeBytes, val); }

protected:

    /**
     * Construct an object by reading the configuration from the input
     * configuration store.
     *
     * @param configStore - reference to a configuration store object
     *
     * @throw std::runtime_error if the input configuration is not consistent
     * with expectations of the application
     */
    explicit ConfigurationStore(util::ConfigStore const& configStore);

private:

    /**
     * Read and validate input configuration parameters from the specified 
     * store and initialize the object.
     *
     * @param configStore - reference to a configuration store object
     *
     * @throw std::runtime_error if the input configuration is not consistent
     * with expectations of the application
     */
    void loadConfiguration(util::ConfigStore const& configStore);

    /**
     * The setter method for numeric types
     * 
     * @param var
     *   a reference to a parameter variable to be set
     * 
     * @param val
     *   the new value of the parameter
     * 
     * @param allowZero
     *   (optional) flag disallowing (if set) zero values
     */
    template <class T>
    void _set(T& var, T val, bool allowZero=false) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << val);
        util::Lock lock(_mtx, context() + __func__);
        if (not allowZero and val == 0) {
            throw std::invalid_argument(
                    "ConfigurationStore::" + std::string(__func__) + "<numeric>  0 value is not allowed");
        }
        var = val;
    }

    /**
     * Specialized version of the setter method for type 'bool'
     */
    void _set(bool& var, bool val) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << (val ? "true" : "false"));
        util::Lock lock(_mtx, context() + __func__);
        var = val;
    }

    /**
     * Specialized version of the setter method for type 'std::string'
     * 
     * @param var
     *   a reference to a parameter variable to be set
     * 
     * @param val
     *   the new value of the parameter
     * 
     * @param allowEmpty
     *   (optional) flag disallowing (if set) empty values
     */
    void _set(std::string& var, std::string const& val, bool allowEmpty=false) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  val=" << val);
        util::Lock lock(_mtx, context() + __func__);
        if (not allowEmpty and val.empty()) {
            throw std::invalid_argument(
                    "ConfigurationStore::" + std::string(__func__) + "<string>  empty value is not allowed");
        }
        var = val;
    }

private:

    /// Message logger
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H
