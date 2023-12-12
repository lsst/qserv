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
#ifndef LSST_QSERV_REPLICA_STATUSREQUESTBASE_H
#define LSST_QSERV_REPLICA_STATUSREQUESTBASE_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/Messenger.h"
#include "replica/protocol.pb.h"
#include "replica/RequestMessenger.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class StatusRequestBase represents the base class for a family of requests
 * pulling a status of on-going operation.
 */
class StatusRequestBase : public RequestMessenger {
public:
    typedef std::shared_ptr<StatusRequestBase> Ptr;

    StatusRequestBase() = delete;
    StatusRequestBase(StatusRequestBase const&) = delete;
    StatusRequestBase& operator=(StatusRequestBase const&) = delete;

    ~StatusRequestBase() override = default;

    /// @return an identifier of the target request
    std::string const& targetRequestId() const { return _targetRequestId; }

    /// @return the performance info of the target operation (if available)
    Performance const& targetPerformance() const { return _targetPerformance; }

    std::string toString(bool extended = false) const override;

protected:
    /**
     * Construct the request.
     * @param serviceProvider  Services of the Replicaion framework.
     * @param io_service  Network communication services.
     * @param requestName  The name of a request.
     * @param workerName  The name of a worker node (the one to be affected by the request).
     * @param targetRequestId  An identifier of the target request whose remote status
     *   is going to be inspected.
     * @param targetRequestType  The type of a request affected by the operation.
     * @param priority   A priority level of a request.
     * @param keepTracking  Keep tracking the request before it finishes or fails.
     * @param messenger  A service for communicating with workers.
     */
    StatusRequestBase(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      char const* requestName, std::string const& workerName,
                      std::string const& targetRequestId, ProtocolQueuedRequestType targetRequestType,
                      int priority, bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    /// @see Request::startImpl()
    void startImpl(replica::Lock const& lock) final;

    /// @see Request::awaken()
    void awaken(boost::system::error_code const& ec) final;

    /**
     * Initiate request-specific send.
     * @param lock  A lock on Request::_mtx must be acquired before calling this method.
     */
    virtual void send(replica::Lock const& lock) = 0;

    /**
     * Process the worker response to the requested operation.
     * @param success 'true' indicates a successful response from a worker
     * @param status a response from the worker service (only valid if success is 'true')
     */
    void analyze(bool success, ProtocolStatus status = ProtocolStatus::FAILED);

    /**
     * Initiate request-specific operation with the persistent state service
     * to store replica status.
     */
    virtual void saveReplicaInfo() = 0;

    /// The performance of the target operation (updated by subclasses)
    Performance _targetPerformance;

private:
    /**
     * Serialize request data into a network buffer and send the message to a worker.
     * @param lock  A lock on Request::_mtx must be acquired before calling this method.
     */
    void _sendImpl(replica::Lock const& lock);

    // Input parameters

    std::string const _targetRequestId;

    ProtocolQueuedRequestType const _targetRequestType;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_STATUSREQUESTBASE_H
