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
#ifndef LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H
#define LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H

/// DatabaseServices.h declares:
///
/// class DatabaseServices
/// (see individual class documentation for more information)

// System headers
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "util/Mutex.h"

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

    ~DatabaseServicesMySQL() override = default;

    /**
     * @see DatabaseServices::saveState()
     */
    void saveState(ControllerIdentity const& identity,
                   uint64_t startTime) final;

    /**
     * @see DatabaseServices::saveState()
     */
    void saveState(Job const& job,
                   Job::Options const& options) final;

    /**
     * @see DatabaseServices::updateHeartbeatTime()
     */
     void updateHeartbeatTime(Job const& job) final;

    /**
     * @see DatabaseServices::saveState()
     */
    void saveState(QservMgtRequest const& request,
                   Performance const& performance,
                   std::string const& serverError) final;

    /**
     * @see DatabaseServices::saveState()
     */
    void saveState(Request const& request,
                   Performance const& performance) final;

    /**
     * @see DatabaseServices::updateRequestState()
     */
    void updateRequestState(Request const& request,
                            std::string const& targetRequestId,
                            Performance const& targetRequestPerformance) final;

    /**
     * @see DatabaseServices::saveReplicaInfo()
     */
    void saveReplicaInfo(ReplicaInfo const& info) final;

    /**
     * @see DatabaseServices::saveReplicaInfoCollection()
     */
    void saveReplicaInfoCollection(std::string const& worker,
                                   std::string const& database,
                                   ReplicaInfoCollection const& infoCollection) final;

    /**
     * @see DatabaseServices::findOldestReplica()
     */
    bool findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                            size_t maxReplicas,
                            bool enabledWorkersOnly) const final;

    /**
     * @see DatabaseServices::findReplicas()
     */
    bool findReplicas(std::vector<ReplicaInfo>& replicas,
                      unsigned int chunk,
                      std::string const& database,
                      bool enabledWorkersOnly) const final;

    /**
     * @see DatabaseServices::findWorkerReplicas()
     */
    bool findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            std::string const& worker,
                            std::string const& database) const final;

    /**
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
     * @param lock      - lock on a mutex must be acquired before calling this method
     * @param replicas  - collection of replicas found upon a successful completion
     * @param worker    - worker name (as per the request)
     * @param database  - database name (as per the request)
     * 
     * @return 'true' if the operation has succeeded (even if no replicas were found)
     */
    bool findWorkerReplicasImpl(util::Lock const& lock,
                                std::vector<ReplicaInfo>& replicas,
                                std::string const& worker,
                                std::string const& database) const;

    /**
     * Actual implementation of the replica update algorithm.
     *
     * @param lock - lock on a mutex must be acquired before calling this method
     * @param info - replica to be added/updated or deleted
     */
    void saveReplicaInfoImpl(util::Lock const& lock,
                             ReplicaInfo const& info);

    /**
     * Actual implementation of the multiple replicas update algorithm.
     *
     * @param lock           - lock on a mutex must be acquired before calling this method
     * @param worker         - woker name (as per the request)
     * @param database       - database name (as per the request)
     * @param infoCollection - collection of replicas
     */
    void saveReplicaInfoCollectionImpl(util::Lock const& lock,
                                       std::string const& worker,
                                       std::string const& database,
                                       ReplicaInfoCollection const& infoCollection);

    /**
     * Fetch replicas satisfying the specified query
     *
     * @param lock     - lock on a mutex must be acquired before calling this method
     * @param replicas - collection of replicas to be returned
     * @param query    - SQL query against the corresponding table
     *
     * @return 'true' if the operation has succeeded (even if no replicas were found)
     */
    bool findReplicasImpl(util::Lock const& lock,
                          std::vector<ReplicaInfo>& replicas,
                          std::string const& query) const;

private:

    /// The configuration service
    Configuration::Ptr _configuration;

    /// Database connection
    database::mysql::Connection::Ptr _conn;

    /// Database connection (second instance for nested queries)
    database::mysql::Connection::Ptr _conn2;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable util::Mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H
