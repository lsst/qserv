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
#ifndef LSST_QSERV_REPLICA_REPLICATIONREQUEST_H
#define LSST_QSERV_REPLICA_REPLICATIONREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/protocol.pb.h"
#include "replica/requests/RequestMessenger.h"
#include "replica/util/Common.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Messenger;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ReplicationRequest represents a transient state of requests
 * within the master controller for creating replicas.
 */
class ReplicationRequest : public RequestMessenger {
public:
    typedef std::shared_ptr<ReplicationRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    ReplicationRequest() = delete;
    ReplicationRequest(ReplicationRequest const&) = delete;
    ReplicationRequest& operator=(ReplicationRequest const&) = delete;

    ~ReplicationRequest() final = default;

    std::string const& database() const { return _database; }
    unsigned int chunk() const { return _chunk; }
    std::string const& sourceWorkerName() const { return _sourceWorkerName; }

    /// @return target request specific parameters
    ReplicationRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return request-specific extended data reported upon a successful
     *   completion of the request
     */
    ReplicaInfo const& responseData() const { return _replicaInfo; }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider a host of services for various communications
     * @param io_service BOOST ASIO API
     * @param workerName the identifier of a worker node (the one to be affected by the replication)
     *   at a destination of the chunk
     * @param sourceWorkerName the identifier of a worker node at a source of the chunk
     * @param database the name of a database
     * @param chunk the number of a chunk to replicate (implies all relevant tables)
     * @param allowDuplicate follow a previously made request if the current one duplicates it
     * @param onFinish an optional callback function to be called upon a completion of the request.
     * @param priority a priority level of the request
     * @param keepTracking keep tracking the request before it finishes or fails
     * @param messenger worker messaging service
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& workerName, std::string const& sourceWorkerName,
                      std::string const& database, unsigned int chunk, bool allowDuplicate,
                      CallbackType const& onFinish, int priority, bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    /// @see Request::startImpl()
    void startImpl(replica::Lock const& lock) final;

    /// @see Request::notify()
    void notify(replica::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(replica::Lock const& lock) final;

    /// @see Request::awaken()
    void awaken(boost::system::error_code const& ec) final;

private:
    /// @see ReplicationRequest::create()
    ReplicationRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                       std::string const& workerName, std::string const& sourceWorkerName,
                       std::string const& database, unsigned int chunk, bool allowDuplicate,
                       CallbackType const& onFinish, int priority, bool keepTracking,
                       std::shared_ptr<Messenger> const& messenger);

    /**
     * Send the serialized content of the buffer to a worker
     * @param lock a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(replica::Lock const& lock);

    /**
     * Process the completion of the requested operation
     * @param success 'true' indicates a successful response from a worker
     * @param message worker response (if success)
     */
    void _analyze(bool success, ProtocolResponseReplicate const& message);

    // Input parameters

    std::string const _database;
    unsigned int const _chunk;
    std::string const _sourceWorkerName;
    CallbackType _onFinish;  ///< @note reset when the request is finished

    /// Request-specific parameters of the target request
    ReplicationRequestParams _targetRequestParams;

    /// Detailed info on the replica status
    ReplicaInfo _replicaInfo;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REPLICATIONREQUEST_H
