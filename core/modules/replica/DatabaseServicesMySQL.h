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
#ifndef LSST_QSERV_REPLICA_DATABASE_SERVICES_MYSQL_H
#define LSST_QSERV_REPLICA_DATABASE_SERVICES_MYSQL_H

/// DatabaseServices.h declares:
///
/// class DatabaseServices
/// (see individual class documentation for more information)

// System headers
#include <vector>

// Qserv headers
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class DatabaseServicesMySQL is a MySQL_specific implementation of the database
  * services for replication entities: Controller, Job and Request.
  *
  * @see class DatabaseServices
  */
class DatabaseServicesMySQL
    :   public DatabaseServices {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServicesMySQL> pointer;

    // Default construction and copy semantics are prohibited

    DatabaseServicesMySQL () = delete;
    DatabaseServicesMySQL (DatabaseServicesMySQL const&) = delete;
    DatabaseServicesMySQL& operator= (DatabaseServicesMySQL const&) = delete;

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit DatabaseServicesMySQL (Configuration::pointer const& configuration);

    /// Destructor
    ~DatabaseServicesMySQL () override = default;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState (ControllerIdentity const& identity,
                    uint64_t                  startTime) override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState (Job_pointer const& job) override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState (Request_pointer const& request) override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findOldestReplica()
     */
    bool findOldestReplicas (std::vector<ReplicaInfo>& replicas,
                             size_t                    maxReplicas,
                             bool                      enabledWorkersOnly) const override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findReplicas()
     */
    bool findReplicas (std::vector<ReplicaInfo>& replicas,
                       unsigned int              chunk,
                       std::string const&        database,
                       bool                      enabledWorkersOnly) const override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicas (std::vector<ReplicaInfo>& replicas,
                             std::string const&        worker,
                             std::string const&        database) const override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicas (std::vector<ReplicaInfo>& replicas,
                             unsigned int              chunk,
                             std::string const&        worker,
                             std::string const&        databaseFamily) const override;
                             
private:

    /**
     * Thread unsafe implementation of the corresponiding public method.
     * This operation is supposed to be invoken in a context where proper
     * thread safety synchronization has been taken care of.
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicasNoLock (std::vector<ReplicaInfo>& replicas,
                                   std::string const&        worker,
                                   std::string const&        database) const;

    /**
     * Update the status of replica in the corresponidng tables. Actual actions
     * would depend on a type of the request:
     *
     * - the replica info (if present) will be removed for the REPLICA_CREATE and
     *   the coresponding Status* and Stop* requests.
     *
     * - the replica info will be either inserted or updated (if already present
     *   in the database) for he REPLICA_DELETE and the coresponding Status*
     *   and Stop* requests.
     *
     * @param info - a replica to be added/updated or deleted
     */
    void saveReplicaInfo (ReplicaInfo const& info);

    /**
     * Update the status of multiple replicas using a collection reported
     * by a request. The method will cross-check replicas reported by the
     * request in a context of the specific worker and a database and resync
     * the database state in this context. Specifically, this means
     * the following:
     *
     * - replicas not present in the colleciton will be deleted from the database
     * - new replicas not present in the database will be registered in there
     * - existing replicas will be updated in the database
     *
     * @see DatabaseServiceseMySQL::saveReplicaInfo()
     *
     * @param worker         - the name of a worker (as per the request)
     * @param database       - the name of a database (as per the request)
     * @param infoCollection - a collection of replicas
     */
    void saveReplicaInfoCollection (std::string const&           worker,
                                    std::string const&           database,
                                    ReplicaInfoCollection const& infoCollection);

    /**
     * Fetch replicas satisfying the specified query
     *
     * @param replicas - a collection of replicas to be returned
     * @param query    - an SQL query against the corresponding table
     *
     * @return 'true' if the operation has succeeded (even if no replicas were found)
     */
    bool findReplicas (std::vector<ReplicaInfo>& replicas,
                       std::string const&        query) const;

protected:

    /// Databse connection
    database::mysql::Connection::pointer _conn;

    /// Databse connection (second instance for nested queries)
    database::mysql::Connection::pointer _conn2;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASE_SERVICES_MYSQL_H