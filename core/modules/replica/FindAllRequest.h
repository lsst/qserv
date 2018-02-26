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
#ifndef LSST_QSERV_REPLICA_FINDALLREQUEST_H
#define LSST_QSERV_REPLICA_FINDALLREQUEST_H

/// FindAllRequest.h declares:
///
/// Common classes shared by both implementations:
///
///   class FindAllRequest
///
/// Request implementations based on individual connectors provided by
/// base class RequestConnection:
///
///   class FindAllRequestC
///
/// Request implementations based on multiplexed connectors provided by
/// base class RequestMessenger:
///
///   class FindAllRequestM
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
#include "replica/RequestConnection.h"
#include "replica/RequestMessenger.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations

class Messenger;


// =============================================
//   Classes based on the dedicated connectors
// =============================================

/**
  * Class FindAllRequestC represents known replicas lookup requests within
  * the master controller.
  */
class FindAllRequestC
    :   public RequestConnection  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FindAllRequestC> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    FindAllRequestC () = delete;
    FindAllRequestC (FindAllRequestC const&) = delete;
    FindAllRequestC& operator= (FindAllRequestC const&) = delete;

    /// Destructor
    ~FindAllRequestC () final;

    // Trivial acccessors
 
    std::string const& database () const { return _database; }

    /// Return target request specific parameters
    FindAllRequestParams const& targetRequestParams () const { return _targetRequestParams; }

    /**
     * Return a refernce to a result of the completed request.
     *
     * Note that this operation will return a sensible result only if the operation
     * finishes with status FINISHED::SUCCESS
     */
    ReplicaInfoCollection const& responseData () const;

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one where the chunks
     *                           expected to be located)
     * @param database         - the name of a database
     * @param onFinish         - an optional callback function to be called upon a completion of the request.
     * @param priority         - a priority level of the request
     * @param keepTracking     - keep tracking the request before it finishes or fails
     */
    static pointer create (ServiceProvider&         serviceProvider,
                           boost::asio::io_service& io_service,
                           std::string const&       worker,
                           std::string const&       database,
                           callback_type            onFinish,
                           int                      priority,
                           bool                     keepTracking);

private:

    /**
     * Construct the request with the pointer to the services provider.
     */
    FindAllRequestC (ServiceProvider&         serviceProvider,
                     boost::asio::io_service& io_service,
                     std::string const&       worker,
                     std::string const&       database,
                     callback_type            onFinish,
                     int                      priority,
                     bool                     keepTracking);

    /**
      * This method is called when a connection is established and
      * the stack is ready to begin implementing an actual protocol
      * with the worker server.
      *
      * The first step of teh protocol will be to send the replication
      * request to the destination worker.
      */
    void beginProtocol () final;
    
    /// Callback handler for the asynchronious operation
    void requestSent (boost::system::error_code const& ec,
                      size_t                           bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (boost::system::error_code const& ec,
                           size_t                           bytes_transferred);

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait ();

    /// Callback handler for the asynchronious operation
    void awaken (boost::system::error_code const& ec);

    /// Start sending the status request to the destination worker
    void sendStatus ();

    /// Callback handler for the asynchronious operation
    void statusSent (boost::system::error_code const& ec,
                     size_t                           bytes_transferred);

    /// Start receiving the status response from the destination worker
    void receiveStatus ();

    /// Callback handler for the asynchronious operation
    void statusReceived (boost::system::error_code const& ec,
                         size_t                           bytes_transferred);

    /// Process the completion of the requested operation
    void analyze (lsst::qserv::proto::ReplicationResponseFindAll const& message);

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void notify () final;

private:

    // Parameters of the object

    std::string _database;

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;

    /// Request-specific parameters of the target request
    FindAllRequestParams _targetRequestParams;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};


// ===============================================
//   Classes based on the multiplexed connectors
// ===============================================

/**
  * Class FindAllRequestM represents known replicas lookup requests within
  * the master controller.
  */
class FindAllRequestM
    :   public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FindAllRequestM> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    FindAllRequestM () = delete;
    FindAllRequestM (FindAllRequestM const&) = delete;
    FindAllRequestM& operator= (FindAllRequestM const&) = delete;

    /// Destructor
    ~FindAllRequestM () final;

    // Trivial acccessors
 
    std::string const& database () const { return _database; }

    /// Return target request specific parameters
    FindAllRequestParams const& targetRequestParams () const { return _targetRequestParams; }

    /**
     * Return a refernce to a result of the completed request.
     *
     * Note that this operation will return a sensible result only if the operation
     * finishes with status FINISHED::SUCCESS
     */
    ReplicaInfoCollection const& responseData () const;

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one where the chunks
     *                           expected to be located)
     * @param database         - the name of a database
     * @param onFinish         - an optional callback function to be called upon a completion of the request.
     * @param priority         - a priority level of the request
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param messenger        - an interface for communicating with workers
     */
    static pointer create (ServiceProvider&                  serviceProvider,
                           boost::asio::io_service&          io_service,
                           std::string const&                worker,
                           std::string const&                database,
                           callback_type                     onFinish,
                           int                               priority,
                           bool                              keepTracking,
                           std::shared_ptr<Messenger> const& messenger);

private:

    /**
     * Construct the request with the pointer to the services provider.
     */
    FindAllRequestM (ServiceProvider&                  serviceProvider,
                     boost::asio::io_service&          io_service,
                     std::string const&                worker,
                     std::string const&                database,
                     callback_type                     onFinish,
                     int                               priority,
                     bool                              keepTracking,
                     std::shared_ptr<Messenger> const& messenger);



    /**
      * Implement the method declared in the base class
      *
      * @see Request::startImpl()
      */
    void startImpl () final;

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait ();

    /// Callback handler for the asynchronious operation
    void awaken (boost::system::error_code const& ec);

    /// Send the serialized content of the buffer to a worker
    void send ();

    /// Process the completion of the requested operation
    void analyze (bool                                                  success,
                  lsst::qserv::proto::ReplicationResponseFindAll const& message);

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void notify () final;

private:

    // Parameters of the object

    std::string _database;

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;

    /// Request-specific parameters of the target request
    FindAllRequestParams _targetRequestParams;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};


// =================================================================
//   Type switch as per the macro defined in replica/Common.h
// =================================================================

#ifdef LSST_QSERV_REPLICA_REQUEST_BASE_C
typedef FindAllRequestC FindAllRequest;
#else
typedef FindAllRequestM FindAllRequest;
#endif // LSST_QSERV_REPLICA_REQUEST_BASE_C

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FINDALLREQUEST_H