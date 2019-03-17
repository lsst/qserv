// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_WORKERDELETEREQUEST_H
#define LSST_QSERV_REPLICA_WORKERDELETEREQUEST_H

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
  * Class WorkerDeleteRequest represents a context and a state of replica deletion
  * requests within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerDeleteRequest : public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerDeleteRequest> Ptr;

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
     *   the name of a worker
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
     * @param chunk
     *   the chunk whose replicas will be deleted
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database,
                      unsigned int chunk);

    // Default construction and copy semantics are prohibited

    WorkerDeleteRequest() = delete;
    WorkerDeleteRequest(WorkerDeleteRequest const&) = delete;
    WorkerDeleteRequest& operator=(WorkerDeleteRequest const&) = delete;

    ~WorkerDeleteRequest() override = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    unsigned int chunk() const { return _chunk; }
    
    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(proto::ReplicationResponseDelete& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

protected:

    /// @see WorkerDeleteRequest::create()
    WorkerDeleteRequest(ServiceProvider::Ptr const& serviceProvider,
                        std::string const& worker,
                        std::string const& id,
                        int priority,
                        std::string const& database,
                        unsigned int chunk);

    // Input parameters

    std::string  const _database;
    unsigned int const _chunk;

    /// Extended status of the replica deletion request
    ReplicaInfo _replicaInfo;
};

/**
  * Class WorkerDeleteRequestPOSIX provides an actual implementation for
  * the replica deletion based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerDeleteRequestPOSIX : public WorkerDeleteRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerDeleteRequestPOSIX> Ptr;

    /// @see WorkerDeleteRequest::create()
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database,
                      unsigned int chunk);

    // Default construction and copy semantics are prohibited

    WorkerDeleteRequestPOSIX() = delete;
    WorkerDeleteRequestPOSIX(WorkerDeleteRequestPOSIX const&) = delete;
    WorkerDeleteRequestPOSIX& operator=(WorkerDeleteRequestPOSIX const&) = delete;

    ~WorkerDeleteRequestPOSIX() final = default;

    /// @see WorkerDeleteRequest::execute()
    bool execute() final;

private:

    /// @see WorkerDeleteRequestPOSIX::create()
    WorkerDeleteRequestPOSIX(ServiceProvider::Ptr const& serviceProvider,
                             std::string const& worker,
                             std::string const& id,
                             int priority,
                             std::string const& database,
                             unsigned int chunk);
};

/**
 * Class WorkerDeleteRequestFS has the same implementation as the 'typedef'-ed
 * class for the replica deletion based on the direct manipulation of files on
 * a POSIX file system.
 */
typedef WorkerDeleteRequestPOSIX WorkerDeleteRequestFS;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERDELETEREQUEST_H
