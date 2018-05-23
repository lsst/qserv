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
#ifndef LSST_QSERV_REPLICA_DATABASESERVICES_H
#define LSST_QSERV_REPLICA_DATABASESERVICES_H

/// DatabaseServices.h declares:
///
/// class DatabaseServices
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/ReplicaInfo.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Configuration;
struct ControllerIdentity;
class Job;
class QservMgtRequest;
class Performance;
class Request;

/**
  * Class DatabaseServices is a high-level interface to the database services
  * for replication entities: Controller, Job and Request.
  *
  * This is also a base class for database technology-specific implementations
  * of the service.
  */
class DatabaseServices
    :   public std::enable_shared_from_this<DatabaseServices> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServices> Ptr;

    /// Forward declaration for the smart reference to Job objects
    typedef std::shared_ptr<Configuration> ConfigurationPtr;

    /**
     * The factory method for instantiating a proper service object based
     * on an application configuration.
     *
     * @param configuration - the configuration service
     * @return pointer to the created object
     */
    static Ptr create(ConfigurationPtr const& configuration);

    // Copy semantics is prohibited

    DatabaseServices(DatabaseServices const&) = delete;
    DatabaseServices& operator=(DatabaseServices const&) = delete;

    virtual ~DatabaseServices() = default;

    /**
     * Save the state of the Controller. Note this operation can be called
     * just once for a particular instance of the Controller.
     *
     * @param identity  - a data structure encapsulating a unique identity of
     *                    the Contriller instance.
     * @param startTime - a time (milliseconds since UNIX Epoch) when an instance of
     *                    the Controller was created.
     *
     * @throws std::logic_error - if this Contoller's state is already found in a database
     */
    virtual void saveState(ControllerIdentity const& identity,
                           uint64_t startTime) = 0;

    /**
     * Save the state of the Job. This operation can be called many times for
     * a particular instance of the Job.
     *
     * NOTE: The method will convert a pointer of the base class Job into
     * the final type to avoid type proliferation through this interface.
     *
     * @param job - reference to a Job object
     */
    virtual void saveState(Job const& job) = 0;

    /**
     * Update the heartbeat timestamp for the job's entry
     *
     * @param job - reference to a Job object
     */
     virtual void updateHeartbeatTime(Job const& job) = 0;

    /**
     * Save the state of the QservMgtRequest. This operation can be called many times for
     * a particular instance of the QservMgtRequest.
     *
     * NOTE: The method will convert a pointer of the base class QservMgtRequest into
     * the final type to avoid type proliferation through this interface.
     *
     * @param request - reference to a QservMgtRequest object
     */
    virtual void saveState(QservMgtRequest const& request) = 0;

    /**
     * Save the state of the Request. This operation can be called many times for
     * a particular instance of the Request.
     *
     * NOTE: The method will convert a pointer of the base class Request into
     * the final type to avoid type proliferation through this interface.
     *
     * @param request - reference to a Request object
     */
    virtual void saveState(Request const& request) = 0;

    /**
     * Update a state of a target request.
     *
     * This method is supposed to be called by monitoring requests (State* and Stop*)
     * to update state of the corresponidng target requests.
     *
     * @param request                  - reference to the monitoring Request object
     * @param targetRequestId          - identifier of a target request
     * @param targetRequestPerformance - performance counters of a target request
     *                                   obtained from a worker
     */
    virtual void updateRequestState(Request const& request,
                                    std::string const& targetRequestId,
                                    Performance const& targetRequestPerformance) = 0;

    /**
     * Update the status of replica in the corresponidng tables.
     *
     * @param info - a replica to be added/updated or deleted
     */
    virtual void saveReplicaInfo(ReplicaInfo const& info) = 0;

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
     * @param worker         - worker name (as per the request)
     * @param database       - database name (as per the request)
     * @param infoCollection - collection of replicas
     */
    virtual void saveReplicaInfoCollection(std::string const& worker,
                                           std::string const& database,
                                           ReplicaInfoCollection const& infoCollection) = 0;

    /**
     * Locate replicas which have the oldest verification timestamps.
     * Return 'true' and populate a collection with up to the 'maxReplicas'
     * if any found.
     *
     * ATTENTION: no assumption on a new status of the replica object
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replica            - reference to an object to be initialized
     * @param maxReplicas        - maximum number of replicas to be returned
     * @param enabledWorkersOnly - (optional) if set to 'true' then only consider known
     *                             workers which are enabled in the Configuration
     *
     * @return 'true' in case of success (even if no replicas were found)
     */
    virtual bool findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                    size_t maxReplicas=1,
                                    bool   enabledWorkersOnly=true) const = 0;

    /**
     * Find all replicas for the specified chunk and the database.
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas           - collection of replicas (if any found)
     * @param chunk              - chunk number
     * @param database           - database name
     * @param enabledWorkersOnly - (optional) if set to 'true' then only consider known
     *                             workers which are enabled in the Configuration
     *
     * @return 'true' in case of success (even if no replicas were found)
     *
     * @throw std::invalid_argument - if the database is unknown or empty
     */
    virtual bool findReplicas(std::vector<ReplicaInfo>& replicas,
                              unsigned int chunk,
                              std::string const& database,
                              bool enabledWorkersOnly=true) const = 0;

    /**
     * Find all replicas for the specified worker and a database (or all
     * databases if no specific one is requested).
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas - collection of replicas (if any found)
     * @param worker   - worker name
     * @param database - (optional) atabase name
     *
     * @return 'true' in case of success (even if no replicas were found)
     *
     * @throw std::invalid_argument - if the worker is unknown or its name
     *                                is empty, or if the database family is
     *                                unknown (if provided)
     */
    virtual bool findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    std::string const& worker,
                                    std::string const& database=std::string()) const = 0;

    /**
     * Find all replicas for the specified chunk on a worker.
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas       - collection of replicas (if any found)
     * @param chunk          - chunk number
     * @param worker         - worker name of a worker
     * @param databaseFamily - (optional) database family name
     *
     * @return 'true' in case of success (even if no replicas were found)
     *
     * @throw std::invalid_argument - if the worker is unknown or its name is empty,
     *                                or if the database family is unknown (if provided)
     */
    virtual bool findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    unsigned int chunk,
                                    std::string const& worker,
                                    std::string const& databaseFamily=std::string()) const = 0;

protected:

    DatabaseServices() = default;

    /// @return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASESERVICES_H
