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
#include "replica/protocol.pb.h"
#include "replica/ReplicaInfo.h"
#include "replica/WorkerRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerDeleteRequest represents a context and a state of replica deletion
 * requests within the worker servers. It can also be used for testing the framework
 * operation as its implementation won't make any changes to any files or databases.
 *
 * Real implementations of the request processing must derive from this class.
 */
class WorkerDeleteRequest : public WorkerRequest {
public:
    typedef std::shared_ptr<WorkerDeleteRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider provider is needed to access the Configuration of
     *   a setup and for validating the input parameters
     * @param worker The name of a worker. The name must match the worker which
     *   is going to execute the request.
     * @param id an identifier of a client request
     * @param priority indicates the importance of the request
     * @param (optional) onExpired request expiration callback function.
     *   If nullptr is passed as a parameter then the request will never expire.
     * @param (optional) requestExpirationIvalSec request expiration interval.
     *   If 0 is passed into the method then a value of the corresponding
     *   parameter for the Controller-side requests will be pulled from
     *   the Configuration.
     * @param request ProtoBuf body of the request
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestDelete const& request);

    WorkerDeleteRequest() = delete;
    WorkerDeleteRequest(WorkerDeleteRequest const&) = delete;
    WorkerDeleteRequest& operator=(WorkerDeleteRequest const&) = delete;

    ~WorkerDeleteRequest() override = default;

    // Trivial get methods

    std::string const& database() const { return _request.database(); }

    unsigned int chunk() const { return _request.chunk(); }

    /**
     * Extract request status into the Protobuf response object.
     * @param response Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseDelete& response) const;

    bool execute() override;

protected:
    WorkerDeleteRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                        std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                        unsigned int requestExpirationIvalSec, ProtocolRequestDelete const& request);

    // Input parameters

    ProtocolRequestDelete const _request;

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
    typedef std::shared_ptr<WorkerDeleteRequestPOSIX> Ptr;

    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestDelete const& request);

    WorkerDeleteRequestPOSIX() = delete;
    WorkerDeleteRequestPOSIX(WorkerDeleteRequestPOSIX const&) = delete;
    WorkerDeleteRequestPOSIX& operator=(WorkerDeleteRequestPOSIX const&) = delete;

    ~WorkerDeleteRequestPOSIX() final = default;

    bool execute() final;

private:
    WorkerDeleteRequestPOSIX(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                             std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                             unsigned int requestExpirationIvalSec, ProtocolRequestDelete const& request);
};

/**
 * Class WorkerDeleteRequestFS has the same implementation as the 'typedef'-ed
 * class for the replica deletion based on the direct manipulation of files on
 * a POSIX file system.
 */
typedef WorkerDeleteRequestPOSIX WorkerDeleteRequestFS;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERDELETEREQUEST_H
