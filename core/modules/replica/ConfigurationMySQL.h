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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONMYSQL_H
#define LSST_QSERV_REPLICA_CONFIGURATIONMYSQL_H

/**
 * This header defines a class which provides a MySQL back-end
 * for the Configuration service.
 */

// System headers
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationMySQL loads configuration parameters from a database.
  *
  * The implementation of this class:
  *
  *   - ensures all required parameters are found in the database
  *   - sets default values for the optional parameters
  *   - caches parameters in memory
  */
class ConfigurationMySQL
    :   public Configuration {

public:

    /// The function type for converting values into the corresponding SQL sub-expressions
    typedef std::function<std::string(database::mysql::Connection::Ptr const& conn)> SetValueExprFunc;

    /**
     * Dump the input configuration into the text representing the database
     * initialization sequence compatible with the database schema assumed
     * by the current implementation.
     *
     * @param config
     *   input configuration to be dumped
     *
     * @return
     *   the text representation of the configuration
     */
    static std::string dump2init(Configuration::Ptr const& config);

    // Default construction and copy semantics are prohibited

    ConfigurationMySQL() = delete;
    ConfigurationMySQL(ConfigurationMySQL const&) = delete;
    ConfigurationMySQL& operator=(ConfigurationMySQL const&) = delete;

    /**
     * Construct the object by reading the configuration
     * from the specified file.
     *
     * @param connectionParams - connection parameters
     */
    ConfigurationMySQL(database::mysql::ConnectionParams const& connectionParams);

    ~ConfigurationMySQL() final = default;

    /**
     * @see Configuration::prefix()
     */
    virtual std::string prefix() const final;

    /**
     * @see Configuration::configUrl()
     */
    std::string configUrl() const final;

    /**
     * @see Configuration::setRequestBufferSizeBytes()
     */
    void setRequestBufferSizeBytes(size_t val) final {
        _set(_requestBufferSizeBytes,
             "common",
             "request_buf_size_bytes",
             val);
    }

    /**
     * @see Configuration::setRetryTimeoutSec()
     */
    void setRetryTimeoutSec(unsigned int val) final {
        _set(_retryTimeoutSec,
             "common",
             "request_retry_interval_sec",
             val);
    }

    /**
     * @see Configuration::setControllerThreads()
     */
    void setControllerThreads(size_t val) final {
        _set(_controllerThreads,
             "controller",
             "num_threads",
             val);
    }

    /**
     * @see Configuration::setControllerHttpPort()
     */
    void setControllerHttpPort(uint16_t val) final {
        _set(_controllerHttpPort,
             "controller",
             "http_server_port",
             val);
    }

    /**
     * @see Configuration::setControllerHttpThreads()
     */
    void setControllerHttpThreads(size_t val) final {
        _set(_controllerHttpThreads,
             "controller",
             "http_server_threads",
             val);
    }

    /**
     * @see Configuration::setControllerRequestTimeoutSec()
     */
    void setControllerRequestTimeoutSec(unsigned int val) final {
        _set(_controllerRequestTimeoutSec,
             "controller",
             "request_timeout_sec",
             val);
    }

    /**
     * @see Configuration::setJobTimeoutSec()
     */
    void setJobTimeoutSec(unsigned int val) final {
        _set(_jobTimeoutSec,
             "controller",
             "job_timeout_sec",
             val);
    }

    /**
     * @see Configuration::setJobHeartbeatTimeoutSec()
     */
    void setJobHeartbeatTimeoutSec(unsigned int val) final {
        _set(_jobHeartbeatTimeoutSec,
             "controller",
             "job_heartbeat_sec",
             val,
             true);
    }

    /**
     * @see Configuration::setXrootdAutoNotify()
     */
    void setXrootdAutoNotify(bool val) final {
        _set(_xrootdAutoNotify,
             "xrootd",
             "auto_notify",
             val);
    }

    /**
     * @see Configuration::setXrootdHost()
     */
    void setXrootdHost(std::string const& val) final {
        _set(_xrootdHost,
             "xrootd",
             "host",
             val);
    }

    /**
     * @see Configuration::setXrootdPort()
     */
    void setXrootdPort(uint16_t val) final {
        _set(_xrootdPort,
             "xrootd",
             "port",
             val);
    }

    /**
     * @see Configuration::setXrootdTimeoutSec()
     */
    void setXrootdTimeoutSec(unsigned int val) final {
        _set(_xrootdTimeoutSec,
             "xrootd",
             "request_timeout_sec",
             val);
    }

    /**
     * @see Configuration::setDatabaseServicesPoolSize()
     */
    void setDatabaseServicesPoolSize(size_t val) final {
        _set(_databaseServicesPoolSize,
             "database",
             "services_pool_size",
             val);
    }

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
    WorkerInfo disableWorker(std::string const& name,
                             bool disable) final;

    /**
     * @see Configuration::setWorkerReadOnly()
     */
    WorkerInfo setWorkerReadOnly(std::string const& name,
                                 bool readOnly) final;

    /**
     * @see Configuration::setWorkerSvcHost()
     */
    WorkerInfo setWorkerSvcHost(std::string const& name,
                                std::string const& host) final;

    /**
     * @see Configuration::setWorkerSvcPort()
     */
    WorkerInfo setWorkerSvcPort(std::string const& name,
                                uint16_t port) final;

    /**
     * @see Configuration::setWorkerFsPort()
     */
    WorkerInfo setWorkerFsPort(std::string const& name,
                               uint16_t port) final;

    /**
     * @see Configuration::setWorkerFsHost()
     */
    WorkerInfo setWorkerFsHost(std::string const& name,
                               std::string const& host) final;

    /**
     * @see Configuration::setWorkerDataDir()
     */
    WorkerInfo setWorkerDataDir(std::string const& name,
                                std::string const& dataDir) final;

    /**
     * @see Configuration::setWorkerTechnology()
     */
    void setWorkerTechnology(std::string const& val) final {
        _set(_workerTechnology,
             "worker",
             "technology",
             val);
    }

    /**
     * @see Configuration::setWorkerNumProcessingThreads()
     */
    void setWorkerNumProcessingThreads(size_t val) final {
        _set(_workerNumProcessingThreads,
             "worker",
             "num_svc_processing_threads",
             val);
    }

    /**
     * @see Configuration::setFsNumProcessingThreads()
     */
    void setFsNumProcessingThreads(size_t val) final {
        _set(_fsNumProcessingThreads,
             "worker",
             "num_fs_processing_threads",
             val);
    }

    /**
     * @see Configuration::setWorkerFsBufferSizeBytes()
     */
    void setWorkerFsBufferSizeBytes(size_t val) final {
        _set(_workerFsBufferSizeBytes,
             "worker",
             "fs_buf_size_bytes",
             val);
    }

    /**
     * @see Configuration::addDatabaseFamily()
     */
    DatabaseFamilyInfo addDatabaseFamily(DatabaseFamilyInfo const& info) final;

    /**
     * @see Configuration::deleteDatabaseFamily()
     */
    void deleteDatabaseFamily(std::string const& name) final;


    /**
     * @see Configuration::addDatabase()
     */
    DatabaseInfo addDatabase(DatabaseInfo const& info) final;

    /**
     * @see Configuration::deleteDatabase()
     */
    void deleteDatabase(std::string const& name) final;

    /**
     * @see Configuration::addTable()
     */
    DatabaseInfo addTable(std::string const& database,
                          std::string const& table,
                          bool isPartitioned) final;

    /**
     * @see Configuration::deleteTable()
     */
    DatabaseInfo deleteTable(std::string const& database,
                             std::string const& table) final;

private:

    /**
     * Analyze the configuration and initialize the cache of parameters.
     *
     * The method will throw one of these exceptions:
     *
     *   std::runtime_error
     *      the configuration is not consistent with expectations of the application
     */
    void loadConfiguration();

    /**
     * The actual implementation of method loadConfiguration.
     *
     * @param lock
     *   the lock on a mutex required for the thread safety
     *
     * @param conn
     *   the reference to the database connector
     */
    void loadConfigurationImpl(util::Lock const& lock,
                               database::mysql::Connection::Ptr const& conn);

    /**
     * The setter method for numeric types
     * 
     * @param var
     *   a reference to a parameter variable to be set
     * 
     * @param category
     *   a value of the 'category' field
     *
     * @param param
     *   a value of the 'param' field
     *
     * @param value
     *   the new value of the parameter
     * 
     * @param allowZero
     *   (optional) flag disallowing (if set) zero values
     */
    template <class T>
    void _set(T& var,
              std::string const& category,
              std::string const& param,
              T value,
              bool allowZero=false) {

        if (not allowZero and value == 0) {
            throw std::invalid_argument(
                    "ConfigurationMySQL::" + std::string(__func__) + "<numeric>  0 value is not allowed");
        }
        _setImp(
            category,
            param,
            [&value](database::mysql::Connection::Ptr const& conn) -> std::string {
                return conn->sqlEqual("value", value);
            },
            [&var,&value]() {
                var = value;
            }
        );
    }

    /**
     * Specialized version of the setter method for type 'bool'
     * 
     * @param var
     *   a reference to a parameter variable to be set
     * 
     * @param category
     *   a value of the 'category' field
     *
     * @param param
     *   a value of the 'param' field
     *
     * @param value
     *   the new value of the parameter
     */
    void _set(bool& var,
              std::string const& category,
              std::string const& param,
              bool value) {

        _setImp(
            category,
            param,
            [&value](database::mysql::Connection::Ptr const& conn) -> std::string {
                return conn->sqlEqual<std::string>("value", value ? "1" : "0");
            },
            [&var,&value]() {
                var = value;
            }
        );
    }

    /**
     * Specialized version of the setter method for type 'std::string'
     * 
     * @param var
     *   a reference to a parameter variable to be set
     * 
     * @param category
     *   a value of the 'category' field
     *
     * @param param
     *   a value of the 'param' field
     *
     * @param value
     *   the new value of the parameter
     * 
     * @param allowEmpty
     *   (optional) flag disallowing (if set) empty values
     */
    void _set(std::string& var,
              std::string const& category,
              std::string const& param,
              std::string const& value,
              bool allowEmpty=false) {

        if (not allowEmpty and value.empty()) {
            throw std::invalid_argument(
                    "ConfigurationMySQL::" + std::string(__func__) + "<string>  empty value is not allowed");
        }
        _setImp(
            category,
            param,
            [&value](database::mysql::Connection::Ptr const& conn) -> std::string {
                return conn->sqlEqual("value", value);
            },
            [&var,&value]() {
                var = value;
            }
        );
    }

    /**
     * Database update method for table "config".
     * 
     * @param category
     *   a value of the 'category' field
     *
     * @param param
     *   a value of the 'param' field
     *
     * @param setValueExpr
     *   the lambda function for converting a value of an arbitrary type
     *   into the corresponding SQL sub-expression for updating the 'value' field
     * 
     * @param onSuccess
     *   a function to be called upon successful completion of the update
     *   while a lock on the internal state of the object is being held.
     *   This function is meant to be used to safely update a transient (cached)
     *   value of the corresponding configuration parameter.
     */
    void _setImp(std::string const& category,
                 std::string const& param,
                 SetValueExprFunc const& setValueExprFunc,
                 std::function<void()> const& onSuccess);

private:

    /// Parameters of the connection
    database::mysql::ConnectionParams const _connectionParams;

    /// Message logger
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONMYSQL_H
