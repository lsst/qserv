/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_ECHOREQUEST_H
#define LSST_QSERV_REPLICA_ECHOREQUEST_H

///  .h declares:
//
///   class EchoRequest
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/RequestMessenger.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

/**
  * Class EchoRequest represents Controller-side requests for testing
  * the controller-worker protocol and the worker-side framework.
  * These requests have no side effects.
  */
class EchoRequest
    :   public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<EchoRequest> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    EchoRequest() = delete;
    EchoRequest(EchoRequest const&) = delete;
    EchoRequest& operator=(EchoRequest const&) = delete;

    ~EchoRequest() final = default;

    // Trivial acccessors

    std::string const& data()  const { return _data; }
    uint64_t           delay() const { return _delay; }

    /// @return target request specific parameters
    EchoRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return a refernce to a result obtained from a remote service.
     *
     * Note that this operation will return a sensible result only if the operation
     * finishes with status FINISHED::SUCCESS
     */
    std::string const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - provider of various services
     * @param worker           - identifier of a worker node
     * @param data             - data string to be echoed back by a worker
     * @param delay            - execution time (milliseconds) of the request at worker
     * @param onFinish         - (optional) callback function to call upon completion of the request
     * @param priority         - priority level of the request
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param messenger        - interface for communicating with workers
     *
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& data,
                      uint64_t delay,
                      CallbackType onFinish,
                      int  priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

private:

    /**
     * Construct the request with the pointer to the services provider.
     */
    EchoRequest(ServiceProvider::Ptr const& serviceProvider,
                boost::asio::io_service& io_service,
                std::string const& worker,
                std::string const& data,
                uint64_t delay,
                CallbackType onFinish,
                int  priority,
                bool keepTracking,
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

    /**
     * Callback handler for the asynchronious operation
     *
     * @param ec - error code to be checked
     */
    void awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock - a lock on a mutex must be acquired before calling this method
     */
    void send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     *
     * @param success - flag indicating if the response succeeded
     * @param message - response from a worker (if success)
     */
    void analyze(bool success,
                 lsst::qserv::proto::ReplicationResponseEcho const& message);

    /**
     * @see Request::notifyImpl()
     */
    void notifyImpl() final;

    /**
     * @see Request::savePersistentState()
     */
    void savePersistentState(util::Lock const& lock) final;

    /**
     * @see Request::extendedPersistentState()
     */
    std::string extendedPersistentState(SqlGeneratorPtr const& gen) const final;

private:

    std::string _data;
    uint64_t    _delay;

    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    EchoRequestParams _targetRequestParams;

    /// The results reported by a worker service
    std::string _responseData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_ECHOREQUEST_H
