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
#ifndef LSST_QSERV_REPLICA_REQUEST_H
#define LSST_QSERV_REPLICA_REQUEST_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/requests/RequestI.h"
#include "replica/util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class ProtocolBuffer;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class Request is the specialized base class for a family of requests within
 * the master Replication Controller service. The request use BOOST ASIO for asynchronous
 * operations and Protobuf for message serialization and communication with worker nodes.
 *
 * The class is not intended to be used directly. Instead, it should be subclassed
 * by a specific request type. Objects of the subclassed request type are created
 * by a factory method "createAndStart" that is provided by each such class.
 */
class Request : public RequestI {
public:
    typedef std::shared_ptr<Request> Ptr;

    Request() = delete;
    Request(Request const&) = delete;
    Request& operator=(Request const&) = delete;

    virtual ~Request() = default;

protected:
    /**
     * Construct the request with the pointer to the services provider.
     *
     * @note options 'keepTracking' and 'disposeRequired'
     *   have effect for specific request only.
     *
     * @param controller The Controller associated with the request.
     * @param type The type name of a request (used informally for debugging, and it's
     *   also stored in the persistent state of the Replication system).
     * @param workerName The name of a worker.
     * @param priority A value of the parameter may affect an execution order of
     *   the request by the worker service. It may also affect an order requests
     *   are processed locally. Higher number means higher priority.
     * @param keepTracking Keep tracking the request before it finishes or fails
     * @param disposeRequired The flag indicating of the worker-side request
     *   disposal is needed for a particular request. Normally, it's required for
     *   requests which are queued by workers in its processing queues.
     */
    Request(std::shared_ptr<Controller> const& controller, std::string const& type,
            std::string const& workerName, int priority, bool keepTracking, bool disposeRequired);

    /// @return A pointer to a buffer for data moved over the network.
    std::shared_ptr<ProtocolBuffer> const& buffer() const { return _bufferPtr; }

    /**
     * Initiate the request disposal at the worker server. This method is automatically
     * called upon successful completion of requests for which the flag 'disposeRequired'
     * was set during request object construction. However, the streaming requests
     * that are designed to make more than one trip to the worker under the same request
     * requests. That is normally done to expedite the garbage collection of the worker
     * requests and prevent excessive memory build up (or keeping other resources)
     * at the worker.
     * @param lock The lock on RequestI::mtx must be acquired before calling this method.
     * @param priority The desired priority level of the operation.
     * @param onFinish The optional callback to be called upon the completion of
     *  the request disposal operation.
     */
    virtual void dispose(replica::Lock const& lock, int priority,
                         OnDisposeCallbackType const& onFinish = nullptr);

private:
    /// Buffer for data moved over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REQUEST_H
