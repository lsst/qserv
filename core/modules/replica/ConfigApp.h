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
#ifndef LSST_QSERV_REPLICA_CONFIGAPP_H
#define LSST_QSERV_REPLICA_CONFIGAPP_H

// System headers
#include <limits>
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/Configuration.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ConfigApp implements a tool for inspecting/modifying configuration
 * records stored in the MySQL/MariaDB database.
 */
class ConfigApp
    :   public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ConfigApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc,
                      const char* const argv[]);

    // Default construction and copy semantics are prohibited

    ConfigApp() = delete;
    ConfigApp(ConfigApp const&) = delete;
    ConfigApp& operator=(ConfigApp const&) = delete;

    ~ConfigApp() override = default;

protected:

    /**
     * @see ConfigApp::create()
     */
    ConfigApp(int argc,
              const char* const argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /**
     * Dump the Configuration into the standard output stream
     * 
     * @return
     *   a status code to be returned to the shell
     */
    int _dump() const;

    /**
     * Dump general configuration parameters into the standard output
     * stream as a table.
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpGeneralAsTable(std::string const& indent) const;

    /**
     * Dump workers into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpWorkersAsTable(std::string const& indent) const;

    /**
     * Dump database families into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpFamiliesAsTable(std::string const& indent) const;

    /**
     * Dump databases into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpDatabasesAsTable(std::string const& indent) const;

    /**
     * Dump the Configuration into the standard output stream in  format which could
     * be used for initializing the Configuration, either directly from the INI file,
     * or indirectly via a database.
     * 
     * @return
     *   a status code to be returned to the shell
     */
    int _configInitFile() const;

    /**
     * Update the general configuration parameters
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _updateGeneral();

    /**
     * Update parameters of a worker
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _updateWorker() const;

    /**
     * Add a new worker
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addWorker() const;

    /**
     * Delete an existing worker and all metadata associated with it
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteWorker() const;

    /**
     * Add a new database family
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addFamily();

    /**
     * Delete an existing database family
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteFamily();

    /**
     * Add a new database
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addDatabase();

    /**
     * Delete an existing database
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteDatabase();

    /**
     * Add a new table
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addTable();

    /**
     * Delete an existing table
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteTable();

private:

    /// Logger stream
    LOG_LOGGER _log;
    
    /// The input Configuration
    Configuration::Ptr _config;

    /// The command
    std::string _command;

    /// Configuration URL
    std::string _configUrl;

    /// An optional scope of the command "DUMP"
    std::string _dumpScope;

    /// Show the actual database password when dumping the Configuration
    bool _dumpDbShowPassword{false};

    /// Print vertical separator in tables
    bool _verticalSeparator{false};

    /// Format of an initialization file
    std::string _format;

    /// Parameters of a worker to be updated
    WorkerInfo _workerInfo;

    /// The flag for enabling a select worker
    bool _workerEnable;

    /// The flag for disabling a select worker
    bool _workerDisable;

    /// The flag for turning a worker into the read-only mode
    bool _workerReadOnly;

    /// The flag for turning a worker into the read-write mode
    bool _workerReadWrite;

    // General parameters

    struct {
        std::string const key         = "NET_BUF_SIZE_BYTES";
        std::string const description = "default buffer size for network communications";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setRequestBufferSizeBytes(value);
        }
    } _requestBufferSizeBytes;

    struct {
        std::string const key         = "NET_RETRY_TIMEOUT_SEC";
        std::string const description = "default retry timeout for network communications";
        unsigned int      value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setRetryTimeoutSec(value);
        }
    } _retryTimeoutSec;

    struct {
        std::string const key         = "CONTR_NUM_THREADS";
        std::string const description = "number of threads managed by BOOST ASIO";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerThreads(value);
        }
    } _controllerThreads;

    struct {
        std::string const key         = "CONTR_HTTP_PORT";
        std::string const description = "port number for the controller's HTTP server";
        uint16_t          value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerHttpPort(value);
        }
    } _controllerHttpPort;

    struct {
        std::string const key         = "CONTR_NUM_HTTP_THREADS";
        std::string const description = "number of threads managed by BOOST ASIO for the HTTP server";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerHttpThreads(value);
        }
    } _controllerHttpThreads;

    struct {
        std::string const key         = "CONTR_REQUEST_TIMEOUT_SEC";
        std::string const description = "default timeout for completing worker requests";
        unsigned int      value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerRequestTimeoutSec(value);
        }
    } _controllerRequestTimeoutSec;

    struct {
        std::string const key         = "CONTR_JOB_TIMEOUT_SEC";
        std::string const description = "default timeout for completing jobs";
        unsigned int      value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setJobTimeoutSec(value);
        }
    } _jobTimeoutSec;

    struct {
        std::string const key         = "CONTR_JOB_HEARTBEAT_SEC";
        std::string const description = "heartbeat interval for jobs. A value of 0 disables heartbeats";
        unsigned int      value       = std::numeric_limits<unsigned int>::max();

        void save(Configuration::Ptr const& config) {
            if (value != std::numeric_limits<unsigned int>::max()) {
                config->setJobHeartbeatTimeoutSec(value);
            }
        }
    } _jobHeartbeatTimeoutSec;

    struct {
        std::string const key         = "QSERV_AUTO_NOTIFY";
        std::string const description = "automatically notify Qserv on changes in replica disposition (0 disables this feature)";
        int               value       = -1;

        void save(Configuration::Ptr const& config) {
            if (value >= 0) config->setXrootdAutoNotify(value != 0);
        }
    } _xrootdAutoNotify;

    struct {
        std::string const key         = "XROOTD_HOST";
        std::string const description = "service location (the host name or an IP address) of XRootD/SSI for communications with Qserv";
        std::string       value;

        void save(Configuration::Ptr const& config) {
            if (not value.empty()) config->setXrootdHost(value);
        }
    } _xrootdHost;

    struct {
        std::string const key         = "XROOTD_PORT";
        std::string const description = "port number for the XRootD/SSI service needed for communications with Qserv";
        uint16_t          value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setXrootdPort(value);
        }
    } _xrootdPort;

    struct {
        std::string const key         = "XROOT_COMM_TIMEOUT_SEC";
        std::string const description = "default timeout for communications with Qserv over XRootD/SSI";
        unsigned int      value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setXrootdTimeoutSec(value);
        }
    } _xrootdTimeoutSec;

    struct {
        std::string const key         = "DB_TECHNOLOGY";
        std::string const description = "name of a database technology for the persistent state";
    } _databaseTechnology;

    struct {
        std::string const key         = "DB_HOST";
        std::string const description = "database service location";
    } _databaseHost;

    struct {
        std::string const key         = "DB_PORT";
        std::string const description = "database service port";
    } _databasePort;

    struct {
        std::string const key         = "DB_USER";
        std::string const description = "user account for connecting to the database service";
    } _databaseUser;

    struct {
        std::string const key         = "DB_PASSWORD";
        std::string const description = "password for connecting to the database service";
    } _databasePassword;

    struct {
        std::string const key         = "DB_NAME";
        std::string const description = "the name of the default database schema";
    } _databaseName;

    struct {
        std::string const key         = "DB_SVC_POOL_SIZE";
        std::string const description = "the pool size at the client database services connector";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setDatabaseServicesPoolSize(value);
        }
    } _databaseServicesPoolSize;

    struct {
        std::string const key         = "WORKER_TECHNOLOGY";
        std::string const description = "name of a technology for implementing requests";
        std::string       value;

        void save(Configuration::Ptr const& config) {
            if (not value.empty()) config->setWorkerTechnology(value);
        }
    } _workerTechnology;

    struct {
        std::string const key         = "WORKER_NUM_PROC_THREADS";
        std::string const description = "number of request processing threads in each worker service";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setWorkerNumProcessingThreads(value);
        }
    } _workerNumProcessingThreads;

    struct {
        std::string const key         = "WORKER_FS_NUM_PROC_THREADS";
        std::string const description = "number of request processing threads in each worker's file server";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setFsNumProcessingThreads(value);
        }
    } _fsNumProcessingThreads;

    struct {
        std::string const key         = "WORKER_FS_BUF_SIZE_BYTES";
        std::string const description = "buffer size for file and network operations at worker's file server";
        size_t            value;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setWorkerFsBufferSizeBytes(value);
        }
    } _workerFsBufferSizeBytes;

    /// For database families
    DatabaseFamilyInfo _familyInfo;

    /// For databases
    DatabaseInfo _databaseInfo;

    /// The name of a database"
    std::string _database;

    /// The name of a table"
    std::string _table;

    /// 'false' for the regular tables, 'true' for the partitioned ones
    bool _isPartitioned = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONFIGAPP_H */
