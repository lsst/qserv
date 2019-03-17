/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_WORKERFINDALLREQUEST_H
#define LSST_QSERV_REPLICA_WORKERFINDALLREQUEST_H

// System headers
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/ReplicaInfo.h"
#include "replica/WorkerRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class WorkerFindAllRequest represents a context and a state of replicas lookup
  * requests within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindAllRequest : public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   provider is needed to access the Configuration of a setup
     *   and for validating the input parameters
     *
     * @param worker
     *   the name of a worker. The name must match the worker which
     *   is going to execute the request.
     *
     * @param id
     *   an identifier of a client request
     *
     * @param priority
     *   indicates the importance of the request
     *
     * @param database
     *   the name of a database defines a scope of the replica
     *   lookup operation
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database);

    // Default construction and copy semantics are prohibited

    WorkerFindAllRequest() = delete;
    WorkerFindAllRequest(WorkerFindAllRequest const&) = delete;
    WorkerFindAllRequest& operator=(WorkerFindAllRequest const&) = delete;

    ~WorkerFindAllRequest() override = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(proto::ReplicationResponseFindAll& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

protected:

    /// @see WorkerFindAllRequest::create()
    WorkerFindAllRequest(ServiceProvider::Ptr const& serviceProvider,
                         std::string const& worker,
                         std::string const& id,
                         int priority,
                         std::string const& database);

    // Input parameters

    std::string const _database;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};

/**
  * Class WorkerFindAllRequestPOSIX provides an actual implementation for
  * the replicas lookup based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerFindAllRequestPOSIX : public WorkerFindAllRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequestPOSIX> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * For a description of parameters:
     *
     * @see WorkerFindAllRequest::create()
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database);

    // Default construction and copy semantics are prohibited

    WorkerFindAllRequestPOSIX() = delete;
    WorkerFindAllRequestPOSIX(WorkerFindAllRequestPOSIX const&) = delete;
    WorkerFindAllRequestPOSIX& operator=(WorkerFindAllRequestPOSIX const&) = delete;

    ~WorkerFindAllRequestPOSIX() final = default;

    /// @see WorkerRequest::execute
    bool execute() final;

private:

    /// @see WorkerFindAllRequestPOSIX::create()
    WorkerFindAllRequestPOSIX(ServiceProvider::Ptr const& serviceProvider,
                              std::string const& worker,
                              std::string const& id,
                              int priority,
                              std::string const& database);
};

/**
 * Class WorkerFindAllRequestFS has the same implementation as the 'typedef'-ed
 * class for the replica deletion based on the direct manipulation of files on
 * a POSIX file system.
 */
typedef WorkerFindAllRequestPOSIX WorkerFindAllRequestFS;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERFINDALLREQUEST_H
