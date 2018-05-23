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
#ifndef LSST_QSERV_REPLICA_DELETEREQUEST_H
#define LSST_QSERV_REPLICA_DELETEREQUEST_H

/// DeleteRequest.h declares:
///
///   class DeleteRequest
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <memory>
#include <string>

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
  * Class DeleteRequest represents a transient state of the replica deletion
  * requests within the master controller for deleting replicas.
  */
class DeleteRequest
    :   public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteRequest> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    DeleteRequest() = delete;
    DeleteRequest(DeleteRequest const&) = delete;
    DeleteRequest& operator=(DeleteRequest const&) = delete;

    ~DeleteRequest() final = default;

    // Trivial acccessors

    std::string const& database() const { return _database; }
    unsigned int       chunk() const    { return _chunk; }

    /// @return parameters of a target request
    DeleteRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return request-specific extended data reported upon a successfull
     * completion of the request
     */
    ReplicaInfo const& responseData() const { return _replicaInfo; }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one where the chunk is supposed
     *                           to be located) at a destination of the chunk
     * @param database         - the name of a database
     * @param chunk            - the number of a chunk to replicate (implies all relevant tables)
     * @param onFinish         - an optional callback function to be called upon a completion of the request.
     * @param priority         - a priority level of the request
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param allowDuplicate   - follow a previously made request if the current one duplicates it
     * @param messenger        - an interface for communicating with workers
     *
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      unsigned int  chunk,
                      CallbackType onFinish,
                      int  priority,
                      bool keepTracking,
                      bool allowDuplicate,
                      std::shared_ptr<Messenger> const& messenger);

private:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @see DeleteRequest::create()
     */
    DeleteRequest(ServiceProvider::Ptr const& serviceProvider,
                  boost::asio::io_service& io_service,
                  std::string const& worker,
                  std::string const& database,
                  unsigned int  chunk,
                  CallbackType onFinish,
                  int  priority,
                  bool keepTracking,
                  bool allowDuplicate,
                  std::shared_ptr<Messenger> const& messenger);

    /**
      * @see Request::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
     * Start the timer before attempting the previously failed
     * or successfull (if a status check is needed) step.
     *
     * @param lock - a lock on a mutex must be acquired before calling this method
     */
    void wait(util::Lock const& lock);

    /// Callback handler for the asynchronious operation
    void awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock - a lock on a mutex must be acquired before calling this method
     */
    void send(util::Lock const& lock);

    /**
     * Process the worker response to the requested operation.
     *
     * @param success - the flag indicating if the operation was successfull
     * @param message - a response from the worker service (if success is 'true')
     */
    void analyze(bool success,
                 proto::ReplicationResponseDelete const& message);

    /**
     * @see Request::notifyImpl()
     */
    void notifyImpl() final;

    /**
     * @see Request::savePersistentState()
     */
    void savePersistentState() final;

    /**
     * @see Request::extendedPersistentState()
     */
    std::string extendedPersistentState(SqlGeneratorPtr const& gen) const final;

private:

    std::string  _database;
    unsigned int _chunk;

    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    DeleteRequestParams _targetRequestParams;

    /// Extended information on a status of the operation
    ReplicaInfo _replicaInfo;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DELETEREQUEST_H
