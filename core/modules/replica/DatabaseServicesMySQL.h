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
#include "replica/Job.h"
#include "replica/QservMgtRequest.h"
#include "replica/Request.h"

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
    typedef std::shared_ptr<DatabaseServicesMySQL> Ptr;

    // Default construction and copy semantics are prohibited

    DatabaseServicesMySQL() = delete;
    DatabaseServicesMySQL(DatabaseServicesMySQL const&) = delete;
    DatabaseServicesMySQL& operator=(DatabaseServicesMySQL const&) = delete;

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit DatabaseServicesMySQL(Configuration::Ptr const& configuration);

    /// Destructor
    ~DatabaseServicesMySQL() override = default;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState(ControllerIdentity const& identity,
                   uint64_t startTime) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState(Job::Ptr const& job) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::updateHeartbeatTime()
     */
     void updateHeartbeatTime(Job::Ptr const& job) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState(QservMgtRequest::Ptr const& request) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveState()
     */
    void saveState(Request::Ptr const& request) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveReplicaInfo()
     */
    void saveReplicaInfo(ReplicaInfo const& info) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveReplicaInfoCollection()
     */
    void saveReplicaInfoCollection(std::string const& worker,
                                   std::string const& database,
                                   ReplicaInfoCollection const& infoCollection) final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findOldestReplica()
     */
    bool findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                            size_t maxReplicas,
                            bool enabledWorkersOnly) const final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findReplicas()
     */
    bool findReplicas(std::vector<ReplicaInfo>& replicas,
                      unsigned int chunk,
                      std::string const& database,
                      bool enabledWorkersOnly) const final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            std::string const& worker,
                            std::string const& database) const final;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            unsigned int chunk,
                            std::string const& worker,
                            std::string const& databaseFamily) const final;

private:

    /**
     * Thread unsafe implementation of the corresponiding public method.
     * This operation is supposed to be invoken in a context where proper
     * thread safety synchronization has been taken care of.
     *
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicasNoLock(std::vector<ReplicaInfo>& replicas,
                                  std::string const& worker,
                                  std::string const&  database) const;

    /**
     * Actual implementation of the replica update algorithm.
     *
     * @param info - a replica to be added/updated or deleted
     */
    void saveReplicaInfoNoLock(ReplicaInfo const& info);

    /**
     * Actual implementation of the multiple replicas update algorithm.
     *
     * @param worker         - the name of a worker (as per the request)
     * @param database       - the name of a database (as per the request)
     * @param infoCollection - a collection of replicas
     */
    void saveReplicaInfoCollectionNoLock(std::string const& worker,
                                         std::string const& database,
                                         ReplicaInfoCollection const& infoCollection);

    /**
     * Fetch replicas satisfying the specified query
     *
     * @param replicas - a collection of replicas to be returned
     * @param query    - an SQL query against the corresponding table
     *
     * @return 'true' if the operation has succeeded (even if no replicas were found)
     */
    bool findReplicas(std::vector<ReplicaInfo>& replicas,
                      std::string const& query) const;

protected:

    /// Databse connection
    database::mysql::Connection::Ptr _conn;

    /// Databse connection (second instance for nested queries)
    database::mysql::Connection::Ptr _conn2;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASE_SERVICES_MYSQL_H
