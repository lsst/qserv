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
#ifndef LSST_QSERV_REPLICA_DELETEREQUEST_H
#define LSST_QSERV_REPLICA_DELETEREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Messenger;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class DeleteRequest represents a transient state of the replica deletion
  * requests within the master controller for deleting replicas.
  */
class DeleteRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    DeleteRequest() = delete;
    DeleteRequest(DeleteRequest const&) = delete;
    DeleteRequest& operator=(DeleteRequest const&) = delete;

    ~DeleteRequest() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    unsigned int       chunk() const    { return _chunk; }

    /// @return parameters of a target request
    DeleteRequestParams const& targetRequestParams() const { return _targetRequestParams; }

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
     * @param worker
     *   the identifier of a worker node (the one where the chunk is supposed
     *   to be located) at a destination of the chunk
     * @param database
     *   the name of a database
     *
     * @param chunk
     *   the number of a chunk to replicate (implies all relevant tables)
     *
     * @param allowDuplicate
     *   follow a previously made request if the current one duplicates it
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
     * @param messenger
     *   an interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      unsigned int chunk,
                      bool allowDuplicate,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;


private:

    /// @see DeleteRequest::create()
    DeleteRequest(ServiceProvider::Ptr const& serviceProvider,
                  boost::asio::io_service& io_service,
                  std::string const& worker,
                  std::string const& database,
                  unsigned int chunk,
                  bool allowDuplicate,
                  CallbackType const& onFinish,
                  int priority,
                  bool keepTracking,
                  std::shared_ptr<Messenger> const& messenger);

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /// Callback handler for the asynchronous operation
    void _awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the worker response to the requested operation.
     *
     * @param success
     *   'true' indicates a successful response from a worker
     *
     * @param message
     *   a response from the worker service (if success is 'true')
     */
    void _analyze(bool success,
                  ProtocolResponseDelete const& message);


    // Input parameters

    std::string  const _database;
    unsigned int const _chunk;
    CallbackType       _onFinish;   /// @note is reset when the job finishes

    /// Request-specific parameters of the target request
    DeleteRequestParams _targetRequestParams;

    /// Extended information on a status of the operation
    ReplicaInfo _replicaInfo;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DELETEREQUEST_H
