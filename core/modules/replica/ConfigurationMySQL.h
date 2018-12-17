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
#include <string>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"

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
     * @see Configuration::setWorkerFsPort()
     */
    WorkerInfo const setWorkerFsPort(std::string const& name,
                                     uint16_t port) final;

    /**
     * @see Configuration::setWorkerFsHost()
     */
    WorkerInfo const setWorkerFsHost(std::string const& name,
                                     std::string const& host) final;

    /**
     * @see Configuration::setWorkerDataDir()
     */
    WorkerInfo const setWorkerDataDir(std::string const& name,
                                      std::string const& dataDir) final;

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

private:

    /// Parameters of the connection
    database::mysql::ConnectionParams const _connectionParams;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONMYSQL_H
