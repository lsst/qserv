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
#ifndef LSST_QSERV_REPLICA_SERVICE_PROVIDER_H
#define LSST_QSERV_REPLICA_SERVICE_PROVIDER_H

/// ServiceProvider.h declares:
///
/// class ServiceProvider
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/ChunkLocker.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Configuration;
class DatabaseServices;
class QservMgtServices;

/**
  * Class ServiceProvider hosts various serviceses for the master server.
  */
class ServiceProvider
    :   public std::enable_shared_from_this<ServiceProvider> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceProvider> pointer;

    // Forward definition for pointer types of the owned services
 
    typedef std::shared_ptr<Configuration>    Configuration_pointer;
    typedef std::shared_ptr<DatabaseServices> DatabaseServices_pointer;
    typedef std::shared_ptr<QservMgtServices> QservMgtServices_pointer;
    
    // Default construction and copy semantics are prohibited

    ServiceProvider() = delete;
    ServiceProvider(ServiceProvider const&) = delete;
    ServiceProvider& operator=(ServiceProvider const&) = delete;

    /**
     * Static factory for creating  the object.
     *
     * @param configUrl - a source of the application configuration parameters
     * @return pointer to the instance
     */
    static ServiceProvider::pointer create(std::string const& configUrl);

    /// Detructor
    ~ServiceProvider() = default;

    /**
     * @return a reference to the configuration service
     */
    Configuration_pointer const& config() const { return _configuration; }

    /**
     * @return a reference to the database services
     */
    DatabaseServices_pointer const& databaseServices() const { return _databaseServices; }

    /**
     * @return a reference to the local (process) chunk locking services
     */
    ChunkLocker& chunkLocker() { return _chunkLocker; }

    /**
     * @return a reference to the Qserv notification services
     */
    QservMgtServices_pointer const& qservMgtServices() const { return _qservMgtServices; }

    /**
     * Make sure this worker is known in the configuration. Throws exception
     * std::invalid_argument otherwise.
     * 
     * @param name - the name of a worker
     */
    void assertWorkerIsValid(std::string const& name);

    /**
     * Make sure workers are now known in the configuration and they're different.
     * Throws exception std::invalid_argument otherwise.
     */
    void assertWorkersAreDifferent(std::string const& workerOneName,
                                   std::string const& workerTwoName);

    /**
     * Make sure this database is known in the configuration. Throws exception
     * std::invalid_argument otherwise.
     *
     * @param name - the name of a database
     */
    void assertDatabaseIsValid(std::string const& name);

private:

    /**
     * Construct the object.
     *
     * @param configUrl - a source of the application configuration parameters
     */
    explicit ServiceProvider(std::string const& configUrl);

private:

    /// Configuration manager
    Configuration_pointer _configuration;

    /// Database services
    DatabaseServices_pointer _databaseServices;

    /// For claiming exclusive ownership over chunks during replication
    /// operations to ensure consistency of the operations.
    ChunkLocker _chunkLocker;

    /// Qserv management services
    QservMgtServices_pointer _qservMgtServices;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SERVICE_PROVIDER_H