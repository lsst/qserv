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
#include <boost/asio.hpp>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

/**
  * Class ReplicationRequest represents a transient state of requests
  * within the master controller for creating replicas.
  */
class ReplicationRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReplicationRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    ReplicationRequest() = delete;
    ReplicationRequest(ReplicationRequest const&) = delete;
    ReplicationRequest& operator=(ReplicationRequest const&) = delete;

    ~ReplicationRequest() final = default;

    // Trivial get methods

    std::string const& database()     const { return _database; }
    unsigned int       chunk()        const { return _chunk; }
    std::string const& sourceWorker() const { return _sourceWorker; }

    /// @return target request specific parameters
    ReplicationRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return
     *   request-specific extended data reported upon a successful
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
     * @param serviceProvider
     *   a host of services for various communications
     *
     * @param io_service
     *   BOOST ASIO API
     *
     * @param worker
     *   the identifier of a worker node (the one to be affected by the replication)
     *   at a destination of the chunk
     *
     * @param sourceWorker
     *   the identifier of a worker node at a source of the chunk
     *
     * @param database
     *   the name of a database
     *
     * @param chunk
     *   the number of a chunk to replicate (implies all relevant tables)
     *
     * @param onFinish
     *   an optional callback function to be called upon a completion of the request.
     *
     * @param priority
     *   a priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param allowDuplicate
     *   follow a previously made request if the current one duplicates it
     *
     * @param messenger
     *   worker messaging service
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& sourceWorker,
                      std::string const& database,
                      unsigned int chunk,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      bool allowDuplicate,
                      std::shared_ptr<Messenger> const& messenger);

    /**
     * @see Request::extendedPersistentState()
     */
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

protected:

    /**
      * @see Request::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
     * @see Request::notify()
     */
    void notify(util::Lock const& lock) final;

    /**
     * @see Request::savePersistentState()
     */
    void savePersistentState(util::Lock const& lock) final;

private:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @see ReplicationRequest::create()
     */
    ReplicationRequest(ServiceProvider::Ptr const& serviceProvider,
                       boost::asio::io_service& io_service,
                       std::string const& worker,
                       std::string const& sourceWorker,
                       std::string const& database,
                       unsigned int  chunk,
                       CallbackType const& onFinish,
                       int priority,
                       bool keepTracking,
                       bool allowDuplicate,
                       std::shared_ptr<Messenger> const& messenger);

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on a mutex must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /// Callback handler for the asynchronous operation
    void _awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock
     *   a lock on a mutex must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     *
     * @param success
     *   completions status of a communication with a worker
     * 
     * @param message
     *   worker response (if success)
     */
    void _analyze(bool success,
                  proto::ReplicationResponseReplicate const& message);

private:

    /// The name of a database
    std::string const _database;

    /// The number of a chunk
    unsigned int const _chunk;

    /// The name of a source worker for the new replica
    std::string const _sourceWorker;

    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    ReplicationRequestParams _targetRequestParams;

    /// Detailed info on the replica status
    ReplicaInfo _replicaInfo;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REPLICATIONREQUEST_H
