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
#ifndef LSST_QSERV_REPLICA_STATUSREQUEST_H
#define LSST_QSERV_REPLICA_STATUSREQUEST_H

/// StatusRequest.h declares:
///
///   class StatusRequest
///   class StatusRequestReplicate
///   class StatusRequestDelete
///   class StatusRequestFind
///   class StatusRequestFindAll
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <future>
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "replica/StatusRequestBase.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

struct StatusReplicationRequestPolicy {

    using ResponseMessageType     = proto::ReplicationResponseReplicate;
    using ResponseDataType        = ReplicaInfo;
    using TargetRequestParamsType = ReplicationRequestParams;

    static char const* requestName();

    static proto::ReplicationReplicaRequestType replicaRequestType();

    static void extractResponseData(ResponseMessageType const& msg,
                                    ResponseDataType& data);

    static void extractTargetRequestParams(ResponseMessageType const& msg,
                                           TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(*request,
                                                                           request->targetRequestId(),
                                                                           request->targetPerformance());
    }
};

struct StatusDeleteRequestPolicy {

    using ResponseMessageType     = proto::ReplicationResponseDelete;
    using ResponseDataType        = ReplicaInfo;
    using TargetRequestParamsType = DeleteRequestParams;

    static char const* requestName();

    static proto::ReplicationReplicaRequestType replicaRequestType();

    static void extractResponseData(ResponseMessageType const& msg,
                                    ResponseDataType& data);

    static void extractTargetRequestParams(ResponseMessageType const& msg,
                                           TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(*request,
                                                                           request->targetRequestId(),
                                                                           request->targetPerformance());
    }
};

struct StatusFindRequestPolicy {

    using ResponseMessageType     = proto::ReplicationResponseFind;
    using ResponseDataType        = ReplicaInfo;
    using TargetRequestParamsType = FindRequestParams;

    static char const* requestName();

    static proto::ReplicationReplicaRequestType replicaRequestType();

    static void extractResponseData(ResponseMessageType const& msg,
                                    ResponseDataType& data);

    static void extractTargetRequestParams(ResponseMessageType const& msg,
                                           TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(*request,
                                                                           request->targetRequestId(),
                                                                           request->targetPerformance());
    }
};

struct StatusFindAllRequestPolicy {

    using ResponseMessageType     = proto::ReplicationResponseFindAll;
    using ResponseDataType        = ReplicaInfoCollection;
    using TargetRequestParamsType = FindAllRequestParams;

    static char const* requestName();

    static proto::ReplicationReplicaRequestType replicaRequestType();

    static void extractResponseData(ResponseMessageType const& msg,
                                    ResponseDataType& data);

    static void extractTargetRequestParams(ResponseMessageType const& msg,
                                           TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfoCollection(
            request->worker(),
            request->targetRequestParams().database,
            request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(*request,
                                                                           request->targetRequestId(),
                                                                           request->targetPerformance());
    }
};

struct StatusEchoRequestPolicy {

    using ResponseMessageType     = proto::ReplicationResponseEcho;
    using ResponseDataType        = std::string;
    using TargetRequestParamsType = EchoRequestParams;

    static char const* requestName();

    static proto::ReplicationReplicaRequestType replicaRequestType();

    static void extractResponseData(ResponseMessageType const& msg,
                                    ResponseDataType& data);

    static void extractTargetRequestParams(ResponseMessageType const& msg,
                                           TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->updateRequestState(*request,
                                                                           request->targetRequestId(),
                                                                           request->targetPerformance());
    }
};

/**
  * Generic class StatusRequest extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StatusRequest
    :   public StatusRequestBase {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequest<POLICY>> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    StatusRequest() = delete;
    StatusRequest(StatusRequest const&) = delete;
    StatusRequest& operator=(StatusRequest const&) = delete;

    ~StatusRequest() final = default;

    /// @return target request specific parameters
    typename POLICY::TargetRequestParamsType const& targetRequestParams() const {
        return _targetRequestParams;
    }

    /**
     * @return request-specific extended data reported upon a successful
     * completion of the request
     */
    typename POLICY::ResponseDataType const& responseData() const { return _responseData; }

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
     *
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& targetRequestId,
                      CallbackType onFinish,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger) {

        return StatusRequest<POLICY>::Ptr(
            new StatusRequest<POLICY>(
                serviceProvider,
                io_service,
                POLICY::requestName(),
                worker,
                targetRequestId,
                POLICY::replicaRequestType(),
                onFinish,
                keepTracking,
                messenger));
    }

private:

    /**
     * Construct the request
     */
    StatusRequest(ServiceProvider::Ptr const& serviceProvider,
                  boost::asio::io_service& io_service,
                  char const* requestName,
                  std::string const& worker,
                  std::string const& targetRequestId,
                  proto::ReplicationReplicaRequestType replicaRequestType,
                  CallbackType onFinish,
                  bool keepTracking,
                  std::shared_ptr<Messenger> const& messenger)
        :   StatusRequestBase(serviceProvider,
                              io_service,
                              requestName,
                              worker,
                              targetRequestId,
                              replicaRequestType,
                              keepTracking,
                              messenger),
            _onFinish(onFinish) {
    }

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void notifyImpl() final {
        if (_onFinish) {
            _onFinish(shared_from_base<StatusRequest<POLICY>>());
        }
    }

    /**
     * Initiate request-specific send
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     *
     * @param lock - a lock on a mutex must be acquired before calling this method
     */
    void send(util::Lock const& lock) final {

        auto self = shared_from_base<StatusRequest<POLICY>>();

        std::shared_ptr<Messenger> const m = messenger();
        m->send<typename POLICY::ResponseMessageType>(
            worker(),
            id(),
            buffer(),
            [self] (std::string const& id,
                    bool success,
                    typename POLICY::ResponseMessageType const& response) {

                if (success) self->analyze(true, self->parseResponse(response));
                else         self->analyze(false);
            }
        );
    }

    /**
     * Initiate request-specific operation with the persistent state
     * service to store replica status.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void saveReplicaInfo() final {
        auto const self = shared_from_base<StatusRequest<POLICY>>();
        POLICY::saveReplicaInfo(self);
    }

    /**
     * Parse request-specific reply
     *
     * @param message - message to parse
     * @return status of the operation reported by a server
     */
    proto::ReplicationStatus parseResponse(
            typename POLICY::ResponseMessageType const& message) {

        // This lock must be acquired because the method is going to modify
        // results of the request. Note that the operation doesn't care
        // about the global state of the request (wether it's already finoshed
        // or not)

        util::Lock lock(_mtx, context() + "parseResponse");

        // Extract target request-specific parameters from the response if available

        POLICY::extractTargetRequestParams(message, _targetRequestParams);

        // Extract request-specific data from the response regardless of
        // the completion status of the request.

        POLICY::extractResponseData(message, _responseData);

        // Always get the latest status reported by the remote server

        setExtendedServerStatus(lock,
                                replica::translate(message.status_ext()));

        // Always update performance counters obtained from the worker service

        mutablePerformance().update(message.performance());

        // Set the optional performance of the target operation

        if (message.has_target_performance()) {
            _targetPerformance.update(message.target_performance());
        }

        // Field 'status' of a type returned by the current method always
        // be defined in all types of request-specific responses.

        return message.status();
    }

private:

    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    typename POLICY::TargetRequestParamsType _targetRequestParams;

    /// Request-specific data
    typename POLICY::ResponseDataType _responseData;
};

typedef StatusRequest<StatusReplicationRequestPolicy> StatusReplicationRequest;
typedef StatusRequest<StatusDeleteRequestPolicy>      StatusDeleteRequest;
typedef StatusRequest<StatusFindRequestPolicy>        StatusFindRequest;
typedef StatusRequest<StatusFindAllRequestPolicy>     StatusFindAllRequest;
typedef StatusRequest<StatusEchoRequestPolicy>        StatusEchoRequest;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STATUSREQUEST_H
