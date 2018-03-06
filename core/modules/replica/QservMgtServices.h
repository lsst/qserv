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
#ifndef LSST_QSERV_REPLICA_QSERV_MGT_SERVICES_H
#define LSST_QSERV_REPLICA_QSERV_MGT_SERVICES_H

/// QservMgtServices.h declares:
///
/// class QservMgtServices
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>

// Qserv headers
#include "replica/Configuration.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations

/**
  * Class QservMgtServices is a high-level interface to the Qserv management
  * services used by the replication system.
  */
class QservMgtServices
    :   public std::enable_shared_from_this<QservMgtServices> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservMgtServices> pointer;

    /// The function type for notifications on the completon of notification
    /// requests on adding new replicas to workers
    typedef std::function<void(pointer,     // pointer to self
                               bool         // 'true' if the operation succeeded
                               )> added_callback_type;

    /**
     * The factory method for instamtiating a proper service object based
     * on an application configuration.
     *
     * @param configuration - the configuration service
     */
    static pointer create(Configuration::pointer const& configuration);

    // Default construction and copy semantics are prohibited

    QservMgtServices() = delete;
    QservMgtServices(QservMgtServices const&) = delete;
    QservMgtServices& operator=(QservMgtServices const&) = delete;

    /// Destructor
    ~QservMgtServices() = default;

    /**
     * Notify Qserv worker on availability of a new chunk
     *
     * @param databaseFamily - the name of a database family involved into the operation
     * @param chunk          - the chunk number
     * @param worker         - the name of a worker where the input replica is residing
     * @param onFinish       - callback function called on a completion of the operation
     */
    void replicaAdded(std::string const& databaseFamily,
                      unsigned int chunk,
                      std::string const& worker,
                      added_callback_type onFinish);

private:

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit QservMgtServices(Configuration::pointer const& configuration);

private:

    /// The configuration service
    Configuration::pointer _configuration;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable std::mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_QSERV_MGT_SERVICES_H