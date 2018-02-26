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
#ifndef LSST_QSERV_REPLICA_STOPREQUEST_H
#define LSST_QSERV_REPLICA_STOPREQUEST_H

/// StopRequest.h declares:
///
/// Common classes shared by both implementations:
///
///   class StopRequestBase
///   class StopRequest
///   class StopRequestReplicate
///   class StopRequestDelete
///   class StopRequestFind
///   class StopRequestFindAll
///
/// Request implementations based on individual connectors provided by
/// base class RequestConnection:
///
///   class StopRequestBaseC
///   class StopRequestC
///   class StopRequestReplicateC
///   class StopRequestDeleteC
///   class StopRequestFindC
///   class StopRequestFindAllC
///
/// Request implementations based on multiplexed connectors provided by
/// base class RequestMessenger:
///
///   class StopRequestBaseM
///   class StopRequestM
///   class StopRequestReplicateM
///   class StopRequestDeleteM
///   class StopRequestFindM
///   class StopRequestFindAllM
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/Messenger.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestConnection.h"
#include "replica/RequestMessenger.h"
#include "replica/ProtocolBuffer.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

struct StopReplicationRequestPolicy {

    static const char* requestTypeName () { return "REQUEST_STOP:REPLICA_CREATE"; } 

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_CREATE; }

    using responseMessageType     = lsst::qserv::proto::ReplicationResponseReplicate;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = ReplicationRequestParams;

    static void extractResponseData (const responseMessageType& msg, responseDataType& data) {
        data = responseDataType(&(msg.replica_info()));
    }
    static void extractTargetRequestParams (const responseMessageType& msg, targetRequestParamsType& params) {
        if (msg.has_request())
            params = targetRequestParamsType(msg.request());
    }
};


struct StopDeleteRequestPolicy {

    static const char* requestTypeName () { return "REQUEST_STOP:REPLICA_DELETE"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_DELETE; }

    using responseMessageType     = lsst::qserv::proto::ReplicationResponseDelete;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = DeleteRequestParams;

    static void extractResponseData (const responseMessageType& msg, responseDataType& data) {
        data = responseDataType(&(msg.replica_info()));
    }
    static void extractTargetRequestParams (const responseMessageType& msg, targetRequestParamsType& params) {
        if (msg.has_request())
            params = targetRequestParamsType(msg.request());
    }
};

struct StopFindRequestPolicy {

    static const char* requestTypeName () { return "REQUEST_STOP:REPLICA_FIND"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_FIND; }

    using responseMessageType     = lsst::qserv::proto::ReplicationResponseFind;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = FindRequestParams;

    static void extractResponseData (const responseMessageType& msg, responseDataType& data) {
        data = responseDataType(&(msg.replica_info()));
    }
    static void extractTargetRequestParams (const responseMessageType& msg, targetRequestParamsType& params) {
        if (msg.has_request())
            params = targetRequestParamsType(msg.request());
    }
};

struct StopFindAllRequestPolicy {

    static const char* requestTypeName () { return "REQUEST_STOP:REPLICA_FIND_ALL"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL; }

    using responseMessageType     = lsst::qserv::proto::ReplicationResponseFindAll;
    using responseDataType        = ReplicaInfoCollection;
    using targetRequestParamsType = FindAllRequestParams;

    static void extractResponseData (const responseMessageType& msg, responseDataType& data) {
        for (int num = msg.replica_info_many_size(), idx = 0; idx < num; ++idx)
            data.emplace_back(&(msg.replica_info_many(idx)));
    }
    static void extractTargetRequestParams (const responseMessageType& msg, targetRequestParamsType& params) {
        if (msg.has_request())
            params = targetRequestParamsType(msg.request());
    }
};


// =============================================
//   Classes based on the dedicated connectors
// =============================================

/**
  * Class StopRequestBaseC represents requests for stopping on-going replications.
  */
class StopRequestBaseC
    :   public RequestConnection  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequestBaseC> pointer;

    // Default construction and copy semantics are prohibited

    StopRequestBaseC () = delete;
    StopRequestBaseC (StopRequestBaseC const&) = delete;
    StopRequestBaseC& operator= (StopRequestBaseC const&) = delete;

    /// Destructor
    ~StopRequestBaseC () override = default;

    /// Return an identifier of the target request
    std::string const& targetRequestId () const { return _targetRequestId; }

    /// Return the performance info of the target operation (if available)
    Performance const& targetPerformance () const { return _targetPerformance; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StopRequestBaseC (ServiceProvider&                                  serviceProvider,
                      boost::asio::io_service&                          io_service,
                      char const*                                       requestTypeName,
                      std::string const&                                worker,
                      std::string const&                                targetRequestId,
                      lsst::qserv::proto::ReplicationReplicaRequestType requestType,
                      bool                                              keepTracking);

    /**
      * This method is called when a connection is established and
      * the stack is ready to begin implementing an actual protocol
      * with the worker server.
      *
      * The first step of the protocol will be to send the replication
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

    /**
     * Parse request-specific reply
     *
     * This method must be implemented by subclasses.
     */
    virtual lsst::qserv::proto::ReplicationStatus parseResponse ()=0;

    /// Process the completion of the requested operation
    void analyze (lsst::qserv::proto::ReplicationStatus status);

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    lsst::qserv::proto::ReplicationReplicaRequestType  _requestType;

protected:

    /// The performance of the target operation
    Performance _targetPerformance;
};


/**
  * Generic class StopRequestC extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StopRequestC
    :   public StopRequestBaseC {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequestC<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    StopRequestC () = delete;
    StopRequestC (StopRequestC const&) = delete;
    StopRequestC& operator= (StopRequestC const&) = delete;

    /// Destructor
    ~StopRequestC () final = default;

    /// Return target request specific parameters
    typename POLICY::targetRequestParamsType const& targetRequestParams () const { return _targetRequestParams; }

    /// Return request-specific extended data reported upon a successfull completion
    /// of the request
    typename POLICY::responseDataType const& responseData () const { return _responseData; }

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one to be affectd by the request)
     * @param io_service       - network communication service
     * @param targetRequestId  - an identifier of the target request whose remote status
     *                           is going to be inspected
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     * @param keepTracking     - keep tracking the request before it finishes or fails
     */
    static pointer create (ServiceProvider         &serviceProvider,
                           boost::asio::io_service &io_service,
                           const std::string       &worker,
                           const std::string       &targetRequestId,
                           callback_type            onFinish,
                           bool                     keepTracking) {

        return StopRequestC<POLICY>::pointer (
            new StopRequestC<POLICY> (
                serviceProvider,
                io_service,
                POLICY::requestTypeName(),
                worker,
                targetRequestId,
                POLICY::requestType(),
                onFinish,
                keepTracking));
    }

private:

    /**
     * Construct the request
     */
    StopRequestC (ServiceProvider                                   &serviceProvider,
                  boost::asio::io_service                           &io_service,
                  const char                                        *requestTypeName,
                  const std::string                                 &worker,
                  const std::string                                 &targetRequestId,
                  lsst::qserv::proto::ReplicationReplicaRequestType  requestType,
                  callback_type                                      onFinish,
                  bool                                               keepTracking)

        :   StopRequestBaseC (serviceProvider,
                              io_service,
                              requestTypeName,
                              worker,
                              targetRequestId,
                              requestType,
                              keepTracking),
            _onFinish (onFinish)
    {}

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void notify () final {
        if (_onFinish != nullptr) {
            StopRequestC<POLICY>::pointer self = shared_from_base<StopRequestC<POLICY>>();
            _onFinish(self);
        }
    }

    /**
     * Parse request-specific reply
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    lsst::qserv::proto::ReplicationStatus parseResponse () final {

        typename POLICY::responseMessageType message;
        _bufferPtr->parse(message, _bufferPtr->size());

        // Extract target request-specific parameters from the response if available
        POLICY::extractTargetRequestParams(message, _targetRequestParams);

        // Extract request-specific data from the response regardless of
        // the completion status of the request.
        POLICY::extractResponseData(message, _responseData);

        // Always get the latest status reported by the remote server
        _extendedServerStatus = replica::translate(message.status_ext());

        // Always update performance counters obtained from the worker service
        _performance.update(message.performance());
        
        // Set the optional performance of the target operation
        if (message.has_target_performance())
            _targetPerformance.update(message.target_performance());

        // Field 'status' of a type returned by the current method always
        // be defined in all types of request-specific responses.

        return message.status();
    }

private:

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;

    /// Request-specific parameters of the target request
    typename POLICY::targetRequestParamsType _targetRequestParams;

    /// Request-specific data
    typename POLICY::responseDataType _responseData;
};


// ===============================================
//   Classes based on the multiplexed connectors
// ===============================================

/**
  * Class StopRequestBaseM represents teh base class for a family of requests
  * stopping an on-going operationd.
  */
class StopRequestBaseM
    :   public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequestBaseM> pointer;

    // Default construction and copy semantics are prohibited

    StopRequestBaseM () = delete;
    StopRequestBaseM (StopRequestBaseM const&) = delete;
    StopRequestBaseM& operator= (StopRequestBaseM const&) = delete;

    /// Destructor
    ~StopRequestBaseM () override = default;

    /// Return an identifier of the target request
    std::string const& targetRequestId () const { return _targetRequestId; }

    /// Return the performance info of the target operation (if available)
    Performance const& targetPerformance () const { return _targetPerformance; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StopRequestBaseM (ServiceProvider&                                  serviceProvider,
                      boost::asio::io_service&                          io_service,
                      char const*                                       requestTypeName,
                      std::string const&                                worker,
                      std::string const&                                targetRequestId,
                      lsst::qserv::proto::ReplicationReplicaRequestType requestType,
                      bool                                              keepTracking,
                      std::shared_ptr<Messenger> const&                 messenger);

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


    /**
     * Initiate request-specific send
     *
     * This method must be implemented by subclasses.
     */
    virtual void send ()=0;

    /**
     * Process the worker response to the requested operation.
     *
     * @param success - the flag indicating if the operation was successfull
     * @param status  - a response from the worker service (only valid if success is 'true')
     */
    void analyze (bool                                  success,
                  lsst::qserv::proto::ReplicationStatus status=lsst::qserv::proto::ReplicationStatus::FAILED);

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    lsst::qserv::proto::ReplicationReplicaRequestType  _requestType;

protected:

    /// The performance of the target operation
    Performance _targetPerformance;
};


/**
  * Generic class StopRequestM extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StopRequestM
    :   public StopRequestBaseM {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequestM<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    StopRequestM () = delete;
    StopRequestM (StopRequestM const&) = delete;
    StopRequestM &operator= (StopRequestM const&) = delete;

    /// Destructor
    ~StopRequestM () final = default;

    /// Return target request specific parameters
    typename POLICY::targetRequestParamsType const& targetRequestParams () const { return _targetRequestParams; }

    /// Return request-specific extended data reported upon asuccessfull completion
    /// of the request
    typename POLICY::responseDataType const& responseData () const { return _responseData; }

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one to be affectd by the request)
     * @param io_service       - network communication service
     * @param targetRequestId  - an identifier of the target request whose remote status
     *                           is going to be inspected
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param messenger        - an interface for communicating with workers
     */
    static pointer create (ServiceProvider&                                  serviceProvider,
                           boost::asio::io_service&                          io_service,
                           std::string const&                                worker,
                           std::string const&                                targetRequestId,
                           callback_type                                     onFinish,
                           bool                                              keepTracking,
                           std::shared_ptr<Messenger> const&                 messenger) {

        return StopRequestM<POLICY>::pointer (
            new StopRequestM<POLICY> (
                serviceProvider,
                io_service,
                POLICY::requestTypeName(),
                worker,
                targetRequestId,
                POLICY::requestType(),
                onFinish,
                keepTracking,
                messenger));
    }

private:

    /**
     * Construct the request
     */
    StopRequestM (ServiceProvider&                                  serviceProvider,
                  boost::asio::io_service&                          io_service,
                  char const*                                       requestTypeName,
                  std::string const&                                worker,
                  std::string const&                                targetRequestId,
                  lsst::qserv::proto::ReplicationReplicaRequestType requestType,
                  callback_type                                     onFinish,
                  bool                                              keepTracking,
                  std::shared_ptr<Messenger> const&                 messenger)

        :   StopRequestBaseM (serviceProvider,
                              io_service,
                              requestTypeName,
                              worker,
                              targetRequestId,
                              requestType,
                              keepTracking,
                              messenger),
            _onFinish (onFinish)
    {}

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void notify () final {
        if (_onFinish != nullptr) {
            StopRequestM<POLICY>::pointer self = shared_from_base<StopRequestM<POLICY>>();
            _onFinish(self);
        }
    }

    /**
     * Initiate request-specific send
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void send () final {

        auto self = shared_from_base<StopRequestM<POLICY>>();
    
        _messenger->send<typename POLICY::responseMessageType> (
            worker(),
            id(),
            _bufferPtr,
            [self] (std::string const&                          id,
                    bool                                        success,
                    typename POLICY::responseMessageType const& response) {

                if (success) self->analyze (true, self->parseResponse(response));
                else         self->analyze (false);
            }
        );
    }

    /**
     * Parse request-specific reply
     *
     * @param message - message to parse
     * @return status of the operation reported by a server
     */
    lsst::qserv::proto::ReplicationStatus parseResponse (
            typename POLICY::responseMessageType const& message) {

        // Extract target request-specific parameters from the response if available
        POLICY::extractTargetRequestParams(message, _targetRequestParams);
 
        // Extract request-specific data from the response regardless of
        // the completion status of the request.
        POLICY::extractResponseData(message, _responseData);

        // Always get the latest status reported by the remote server
        _extendedServerStatus = replica::translate(message.status_ext());

        // Always update performance counters obtained from the worker service
        _performance.update(message.performance());

        // Set the optional performance of the target operation
        if (message.has_target_performance())
            _targetPerformance.update(message.target_performance());

        // Field 'status' of a type returned by the current method always
        // be defined in all types of request-specific responses.

        return message.status();
    }

private:

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;

    /// Request-specific parameters of the target request
    typename POLICY::targetRequestParamsType _targetRequestParams;

    /// Request-specific data
    typename POLICY::responseDataType _responseData;
};


// =================================================================
//   Type switch as per the macro defined in replica/Common.h
// =================================================================

#ifdef LSST_QSERV_REPLICA_REQUEST_BASE_C

typedef StopRequestBaseC StopRequestBase;

typedef StopRequestC<StopReplicationRequestPolicy> StopReplicationRequest;
typedef StopRequestC<StopDeleteRequestPolicy>      StopDeleteRequest;
typedef StopRequestC<StopFindRequestPolicy>        StopFindRequest;
typedef StopRequestC<StopFindAllRequestPolicy>     StopFindAllRequest;

#else  // LSST_QSERV_REPLICA_REQUEST_BASE_C

typedef StopRequestBaseM StopRequestBase;

typedef StopRequestM<StopReplicationRequestPolicy> StopReplicationRequest;
typedef StopRequestM<StopDeleteRequestPolicy>      StopDeleteRequest;
typedef StopRequestM<StopFindRequestPolicy>        StopFindRequest;
typedef StopRequestM<StopFindAllRequestPolicy>     StopFindAllRequest;

#endif // LSST_QSERV_REPLICA_REQUEST_BASE_C

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STOPREQUEST_H