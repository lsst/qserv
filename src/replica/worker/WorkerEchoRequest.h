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
#ifndef LSST_QSERV_REPLICA_WORKERECHOREQUEST_H
#define LSST_QSERV_REPLICA_WORKERECHOREQUEST_H

// System headers
#include <cstdint>
#include <string>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/worker/WorkerRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerEchoRequest implements test requests within the worker servers.
 * Requests of this type don't have any side effects (in terms of modifying
 * any files or databases).
 */
class WorkerEchoRequest : public WorkerRequest {
public:
    typedef std::shared_ptr<WorkerEchoRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider provider is needed to access the Configuration
     *   of a setup and for validating the input parameters
     * @param worker the name of a worker. The name must match the worker which
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
                      unsigned int requestExpirationIvalSec, ProtocolRequestEcho const& request);

    WorkerEchoRequest() = delete;
    WorkerEchoRequest(WorkerEchoRequest const&) = delete;
    WorkerEchoRequest& operator=(WorkerEchoRequest const&) = delete;

    ~WorkerEchoRequest() override = default;

    // Trivial get methods

    std::string const& data() const { return _request.data(); }

    uint64_t delay() const { return _request.delay(); }

    /**
     * Extract request status into the Protobuf response object.
     * @param response Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseEcho& response) const;

    bool execute() override;

protected:
    WorkerEchoRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestEcho const& request);

    // Input parameters

    ProtocolRequestEcho const _request;

    /// The amount of the initial delay which is still left
    uint64_t _delayLeft;
};

/// Class WorkerEchoRequest provides an actual implementation
typedef WorkerEchoRequest WorkerEchoRequestFS;

/// Class WorkerEchoRequest provides an actual implementation
typedef WorkerEchoRequest WorkerEchoRequestPOSIX;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERECHOREQUEST_H
