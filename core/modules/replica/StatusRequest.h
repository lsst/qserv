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
#ifndef LSST_QSERV_REPLICA_STATUS_REQUEST_H
#define LSST_QSERV_REPLICA_STATUS_REQUEST_H

/// StatusRequest.h declares:
///
/// Common classes shared by both implementations:
///
///   class StatusRequestBase
///   class StatusRequest
///   class StatusRequestReplicate
///   class StatusRequestDelete
///   class StatusRequestFind
///   class StatusRequestFindAll
///
/// Request implementations based on individual connectors provided by
/// base class RequestConnection:
///
///   class StatusRequestBaseC
///   class StatusRequestC
///   class StatusRequestReplicateC
///   class StatusRequestDeleteC
///   class StatusRequestFindC
///   class StatusRequestFindAllC
///
/// Request implementations based on multiplexed connectors provided by
/// base class RequestMessenger:
///
///   class StatusRequestBaseM
///   class StatusRequestM
///   class StatusRequestReplicateM
///   class StatusRequestDeleteM
///   class StatusRequestFindM
///   class StatusRequestFindAllM
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

struct StatusReplicationRequestPolicy {

    static char const* requestTypeName () { return "REQUEST_STATUS:REPLICA_CREATE"; } 

    static proto::ReplicationReplicaRequestType requestType () {
        return proto::ReplicationReplicaRequestType::REPLICA_CREATE;
    }

    using responseMessageType     = proto::ReplicationResponseReplicate;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = ReplicationRequestParams;

    static void extractResponseData (responseMessageType const& msg,
                                     responseDataType& data) {
        data = responseDataType(&(msg.replica_info()));
    }
    static void extractTargetRequestParams (responseMessageType const& msg,
                                            targetRequestParamsType& params) {
        if (msg.has_request()) {
            params = targetRequestParamsType(msg.request());
        }
    }
};

struct StatusDeleteRequestPolicy {

    static char const* requestTypeName () { return "REQUEST_STATUS:REPLICA_DELETE"; }

    static proto::ReplicationReplicaRequestType requestType () {
        return proto::ReplicationReplicaRequestType::REPLICA_DELETE;
    }

    using responseMessageType     = proto::ReplicationResponseDelete;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = DeleteRequestParams;

    static void extractResponseData (responseMessageType const& msg,
                                     responseDataType& data) {
        data = responseDataType(&(msg.replica_info()));
    }
    static void extractTargetRequestParams (responseMessageType const& msg,
                                            targetRequestParamsType& params) {
        if (msg.has_request())
            params = targetRequestParamsType(msg.request());
    }
};

struct StatusFindRequestPolicy {

    static char const* requestTypeName () { return "REQUEST_STATUS:REPLICA_FIND"; }

    static proto::ReplicationReplicaRequestType requestType () {
        return proto::ReplicationReplicaRequestType::REPLICA_FIND;
    }

    using responseMessageType     = proto::ReplicationResponseFind;
    using responseDataType        = ReplicaInfo;
    using targetRequestParamsType = FindRequestParams;

    static void extractResponseData(responseMessageType const& msg,
                                    responseDataType& data) {
        data = ReplicaInfo(&(msg.replica_info()));
    }
    static void extractTargetRequestParams(responseMessageType const& msg,
                                           targetRequestParamsType& params) {
        if (msg.has_request()) {
            params = targetRequestParamsType(msg.request());
        }
    }
};

struct StatusFindAllRequestPolicy {

    static char const* requestTypeName() { return "REQUEST_STATUS:REPLICA_FIND_ALL"; }

    static proto::ReplicationReplicaRequestType requestType() {
        return proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL;
    }

    using responseMessageType     = proto::ReplicationResponseFindAll;
    using responseDataType        = ReplicaInfoCollection;
    using targetRequestParamsType = FindAllRequestParams;

    static void extractResponseData (responseMessageType const& msg,
                                     responseDataType& data) {

        for (int num = msg.replica_info_many_size(), idx = 0; idx < num; ++idx) {
            data.emplace_back(&(msg.replica_info_many(idx)));
        }
    }
    static void extractTargetRequestParams (responseMessageType const& msg,
                                            targetRequestParamsType& params) {
        if (msg.has_request()) {
            params = targetRequestParamsType(msg.request());
        }
    }
};

// =============================================
//   Classes based on the dedicated connectors
// =============================================

/**
  * Class StatusRequestBaseC represents the base class for a family of requests
  * pulling a status of on-going operationd.
  */
class StatusRequestBaseC
    :   public RequestConnection {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestBaseC> pointer;

    // Default construction and copy semantics are prohibited

    StatusRequestBaseC() = delete;
    StatusRequestBaseC(StatusRequestBaseC const&) = delete;
    StatusRequestBaseC& operator=(StatusRequestBaseC const&) = delete;

    /// Destructor
    ~StatusRequestBaseC () override = default;

    /// Return an identifier of the target request
    std::string const& targetRequestId () const { return _targetRequestId; }

    /// Return the performance info of the target operation (if available)
    Performance const& targetPerformance () const { return _targetPerformance; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StatusRequestBaseC (ServiceProvider&         serviceProvider,
                        boost::asio::io_service& io_service,
                        char const*              requestTypeName,
                        std::string const&       worker,
                        std::string const&       targetRequestId,
                        proto::ReplicationReplicaRequestType requestType,
                        bool                     keepTracking);

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
                      size_t bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (boost::system::error_code const& ec,
                           size_t bytes_transferred);

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait ();

    /// Callback handler for the asynchronious operation
    void awaken (boost::system::error_code const& ec);

    /// Start sending the status request to the destination worker
    void sendStatus ();

    /// Callback handler for the asynchronious operation
    void statusSent (boost::system::error_code const& ec,
                     size_t bytes_transferred);

    /// Start receiving the status response from the destination worker
    void receiveStatus ();

    /// Callback handler for the asynchronious operation
    void statusReceived (boost::system::error_code const& ec,
                         size_t bytes_transferred);

    /**
     * Parse request-specific reply
     *
     * This method must be implemented by subclasses.
     */
    virtual proto::ReplicationStatus parseResponse ()=0;

    /// Process the completion of the requested operation
    void analyze (proto::ReplicationStatus status);

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    proto::ReplicationReplicaRequestType  _requestType;

protected:

    /// The performance of the target operation
    Performance _targetPerformance;
};

/**
  * Generic class StatusRequestC extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StatusRequestC
    :   public StatusRequestBaseC {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestC<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    StatusRequestC() = delete;
    StatusRequestC(StatusRequestC const&) = delete;
    StatusRequestC& operator=(StatusRequestC const&) = delete;

    /// Destructor
    ~StatusRequestC() final = default;

    /// Return target request specific parameters
    typename POLICY::targetRequestParamsType const& targetRequestParams() const {
        return _targetRequestParams;
    }

    /// Return request-specific extended data reported upon asuccessfull completion
    /// of the request
    typename POLICY::responseDataType const& responseData() const { return _responseData; }

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
    static pointer create (ServiceProvider&         serviceProvider,
                           boost::asio::io_service& io_service,
                           std::string const&       worker,
                           std::string const&       targetRequestId,
                           callback_type            onFinish,
                           bool                     keepTracking) {

        return StatusRequestC<POLICY>::pointer(
            new StatusRequestC<POLICY>(
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
    StatusRequestC (ServiceProvider&         serviceProvider,
                    boost::asio::io_service& io_service,
                    char const*              requestTypeName,
                    std::string const&       worker,
                    std::string const&       targetRequestId,
                    proto::ReplicationReplicaRequestType requestType,
                    callback_type            onFinish,
                    bool                     keepTracking)
        :   StatusRequestBaseC (serviceProvider,
                                io_service,
                                requestTypeName,
                                worker,
                                targetRequestId,
                                requestType,
                                keepTracking),
            _onFinish(onFinish) {
    }

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void notify() final {
        if (_onFinish != nullptr) {
            StatusRequestC<POLICY>::pointer self = shared_from_base<StatusRequestC<POLICY>>();
            _onFinish(self);
        }
    }

    /**
     * Parse request-specific reply
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    proto::ReplicationStatus parseResponse() final {

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
        if (message.has_target_performance()) {
            _targetPerformance.update(message.target_performance());
        }

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
  * Class StatusRequestBaseM represents teh base class for a family of requests
  * pulling a status of on-going operationd.
  */
class StatusRequestBaseM
    :   public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestBaseM> pointer;

    // Default construction and copy semantics are prohibited

    StatusRequestBaseM() = delete;
    StatusRequestBaseM(StatusRequestBaseM const&) = delete;
    StatusRequestBaseM& operator=(StatusRequestBaseM const&) = delete;

    /// Destructor
    ~StatusRequestBaseM() override = default;

    /// Return an identifier of the target request
    std::string const& targetRequestId() const { return _targetRequestId; }

    /// Return the performance info of the target operation (if available)
    Performance const& targetPerformance() const { return _targetPerformance; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StatusRequestBaseM (ServiceProvider&         serviceProvider,
                        boost::asio::io_service& io_service,
                        char const*              requestTypeName,
                        std::string const&       worker,
                        std::string const&       targetRequestId,
                        proto::ReplicationReplicaRequestType requestType,
                        bool                     keepTracking,
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
    void analyze (bool success,
                  proto::ReplicationStatus status=proto::ReplicationStatus::FAILED);

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    proto::ReplicationReplicaRequestType  _requestType;

protected:

    /// The performance of the target operation
    Performance _targetPerformance;
};

/**
  * Generic class StatusRequestM extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StatusRequestM
    :   public StatusRequestBaseM {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestM<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    StatusRequestM () = delete;
    StatusRequestM (StatusRequestM const&) = delete;
    StatusRequestM& operator= (StatusRequestM const&) = delete;

    /// Destructor
    ~StatusRequestM () final = default;

    /// Return target request specific parameters
    typename POLICY::targetRequestParamsType const& targetRequestParams () const {
        return _targetRequestParams;
    }

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
    static pointer create (ServiceProvider&         serviceProvider,
                           boost::asio::io_service& io_service,
                           std::string const&       worker,
                           std::string const&       targetRequestId,
                           callback_type            onFinish,
                           bool                     keepTracking,
                           std::shared_ptr<Messenger> const& messenger) {

        return StatusRequestM<POLICY>::pointer (
            new StatusRequestM<POLICY> (
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
    StatusRequestM (ServiceProvider&         serviceProvider,
                    boost::asio::io_service& io_service,
                    char const*              requestTypeName,
                    std::string const&       worker,
                    std::string const&       targetRequestId,
                    proto::ReplicationReplicaRequestType requestType,
                    callback_type            onFinish,
                    bool                     keepTracking,
                    std::shared_ptr<Messenger> const& messenger)
        :   StatusRequestBaseM (serviceProvider,
                               io_service,
                               requestTypeName,
                               worker,
                               targetRequestId,
                               requestType,
                               keepTracking,
                               messenger),
            _onFinish (onFinish) {
    }

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void notify () final {
        if (_onFinish != nullptr) {
            StatusRequestM<POLICY>::pointer self = shared_from_base<StatusRequestM<POLICY>>();
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
 
        auto self = shared_from_base<StatusRequestM<POLICY>>();
    
        _messenger->send<typename POLICY::responseMessageType> (
            worker(),
            id(),
            _bufferPtr,
            [self] (std::string const& id,
                    bool success,
                    typename POLICY::responseMessageType const& response) {

                if (success) { self->analyze (true, self->parseResponse(response)); }
                else         { self->analyze (false); }
            }
        );
    }

    /**
     * Parse request-specific reply
     *
     * @param message - message to parse
     * @return status of the operation reported by a server
     */
    proto::ReplicationStatus parseResponse (
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
        if (message.has_target_performance()) {
            _targetPerformance.update(message.target_performance());
        }

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

typedef StatusRequestBaseC StatusRequestBase;

typedef StatusRequestC<StatusReplicationRequestPolicy> StatusReplicationRequest;
typedef StatusRequestC<StatusDeleteRequestPolicy>      StatusDeleteRequest;
typedef StatusRequestC<StatusFindRequestPolicy>        StatusFindRequest;
typedef StatusRequestC<StatusFindAllRequestPolicy>     StatusFindAllRequest;

#else  // LSST_QSERV_REPLICA_REQUEST_BASE_C

typedef StatusRequestBaseM StatusRequestBase;

typedef StatusRequestM<StatusReplicationRequestPolicy> StatusReplicationRequest;
typedef StatusRequestM<StatusDeleteRequestPolicy>      StatusDeleteRequest;
typedef StatusRequestM<StatusFindRequestPolicy>        StatusFindRequest;
typedef StatusRequestM<StatusFindAllRequestPolicy>     StatusFindAllRequest;

#endif // LSST_QSERV_REPLICA_REQUEST_BASE_C

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STATUS_REQUEST_H