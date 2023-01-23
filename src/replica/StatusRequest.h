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
#ifndef LSST_QSERV_REPLICA_STATUSREQUEST_H
#define LSST_QSERV_REPLICA_STATUSREQUEST_H

/**
 * This header declares a collection of the request status management request
 * classes for the Controller-side Replication Framework.
 *
 * @see class StatusRequestReplicate
 * @see class StatusRequestDelete
 * @see class StatusRequestFind
 * @see class StatusRequestFindAll
 */

// System headers
#include <functional>
#include <iostream>
#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/protocol.pb.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlResultSet.h"
#include "replica/StatusRequestBase.h"

// This header declarations
namespace lsst::qserv::replica {

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

class StatusReplicationRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseReplicate;
    using ResponseDataType = ReplicaInfo;
    using TargetRequestParamsType = ReplicationRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

class StatusDeleteRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseDelete;
    using ResponseDataType = ReplicaInfo;
    using TargetRequestParamsType = DeleteRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

class StatusFindRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseFind;
    using ResponseDataType = ReplicaInfo;
    using TargetRequestParamsType = FindRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfo(request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

class StatusFindAllRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseFindAll;
    using ResponseDataType = ReplicaInfoCollection;
    using TargetRequestParamsType = FindAllRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->saveReplicaInfoCollection(
                request->worker(), request->targetRequestParams().database, request->responseData());
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

class StatusEchoRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseEcho;
    using ResponseDataType = std::string;
    using TargetRequestParamsType = EchoRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

/// @note This type of the management request for testing the target request won't
///  return any data even in case when the target request was found successfully
///  completed. An assumption here is that since the target requests of this
///  kind have no side effects then they could always be resubmited if needed.
///  The response object will contained the server error should the request failed
///  at a worker.
class StatusDirectorIndexRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseDirectorIndex;
    using ResponseDataType = std::string;
    using TargetRequestParamsType = DirectorIndexRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

class StatusSqlRequestPolicy {
public:
    using ResponseMessageType = ProtocolResponseSql;
    using ResponseDataType = SqlResultSet;
    using TargetRequestParamsType = SqlRequestParams;

    static char const* requestName();
    static ProtocolQueuedRequestType targetRequestType();
    static void extractResponseData(ResponseMessageType const& msg, ResponseDataType& data);
    static void extractTargetRequestParams(ResponseMessageType const& msg, TargetRequestParamsType& params);

    template <class REQUEST_PTR>
    static void saveReplicaInfo(REQUEST_PTR const& request) {
        request->serviceProvider()->databaseServices()->updateRequestState(
                *request, request->targetRequestId(), request->targetPerformance());
    }
};

/**
 * Generic class StatusRequest extends its base class to allow further policy-based
 * customization of specific requests.
 */
template <typename POLICY>
class StatusRequest : public StatusRequestBase {
public:
    typedef std::shared_ptr<StatusRequest<POLICY>> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    StatusRequest() = delete;
    StatusRequest(StatusRequest const&) = delete;
    StatusRequest& operator=(StatusRequest const&) = delete;

    ~StatusRequest() final = default;

    /// @return target request specific parameters
    typename POLICY::TargetRequestParamsType const& targetRequestParams() const {
        return _targetRequestParams;
    }

    /**
     * @return  The request-specific extended data reported upon a successful
     *   completion of the request.
     */
    typename POLICY::ResponseDataType const& responseData() const { return _responseData; }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider a host of services for various communications
     * @param worker the identifier of a worker node (the one to be affected by the request)
     * @param io_service network communication service
     * @param targetRequestId an identifier of the target request whose remote status
     *   is going to be inspected
     * @param onFinish an optional callback function to be called upon a completion of
     *   the request.
     * @param priority priority level of the request
     * @param keepTracking keep tracking the request before it finishes or fails
     * @param messenger an interface for communicating with workers
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, std::string const& targetRequestId,
                      CallbackType const& onFinish, int priority, bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger) {
        return StatusRequest<POLICY>::Ptr(new StatusRequest<POLICY>(
                serviceProvider, io_service, POLICY::requestName(), worker, targetRequestId,
                POLICY::targetRequestType(), onFinish, priority, keepTracking, messenger));
    }

    /**
     * Make an extended print of the request which would include a result set.
     * The method will also make a call to Request::defaultPrinter().
     * @param ptr  An object to be printed.
     */
    static void extendedPrinter(Ptr const& ptr) {
        Request::defaultPrinter(ptr);
        std::cout << ptr->responseData() << std::endl;
    }

protected:
    void notify(replica::Lock const& lock) final {
        notifyDefaultImpl<StatusRequest<POLICY>>(lock, _onFinish);
    }

    /**
     * Initiate request-specific send.
     * @note This method implements the corresponding virtual method defined
     *   by the base class.
     * @param lock  A lock on Request::_mtx must be acquired before calling this method.
     */
    void send(replica::Lock const& lock) final {
        auto self = shared_from_base<StatusRequest<POLICY>>();
        messenger()->send<typename POLICY::ResponseMessageType>(
                worker(), id(), priority(), buffer(),
                [self](std::string const& id, bool success,
                       typename POLICY::ResponseMessageType const& response) {
                    if (success)
                        self->analyze(true, self->_parseResponse(response));
                    else
                        self->analyze(false);
                });
    }

    /**
     * Initiate request-specific operation with the persistent state service
     * to store replica status.
     * @note This method implements the corresponding virtual method defined
     *   by the base class.
     */
    void saveReplicaInfo() final {
        auto const self = shared_from_base<StatusRequest<POLICY>>();
        POLICY::saveReplicaInfo(self);
    }

private:
    StatusRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                  char const* requestName, std::string const& worker, std::string const& targetRequestId,
                  ProtocolQueuedRequestType targetRequestType, CallbackType const& onFinish, int priority,
                  bool keepTracking, std::shared_ptr<Messenger> const& messenger)
            : StatusRequestBase(serviceProvider, io_service, requestName, worker, targetRequestId,
                                targetRequestType, priority, keepTracking, messenger),
              _onFinish(onFinish) {}

    /**
     * Parse request-specific reply.
     * @param message  A message to be parsed.
     * @return  The status of the operation reported by a server.
     */
    ProtocolStatus _parseResponse(typename POLICY::ResponseMessageType const& message) {
        // This lock must be acquired because the method is going to modify
        // results of the request. Note that the operation doesn't care
        // about the global state of the request (wether it's already finished
        // or not)
        replica::Lock lock(_mtx, context() + __func__);

        // Extract target request-specific parameters from the response if available.
        POLICY::extractTargetRequestParams(message, _targetRequestParams);

        // Extract request-specific data from the response regardless of
        // the completion status of the request.
        POLICY::extractResponseData(message, _responseData);

        // Always get the latest status reported by the remote server.
        setExtendedServerStatus(lock, message.status_ext());

        // Always update performance counters obtained from the worker service.
        mutablePerformance().update(message.performance());

        // Set the optional performance of the target operation.
        if (message.has_target_performance()) {
            _targetPerformance.update(message.target_performance());
        }

        // Field 'status' of a type returned by the current method always
        // be defined in all types of request-specific responses.
        return message.status();
    }

    // Input parameters

    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    typename POLICY::TargetRequestParamsType _targetRequestParams;

    /// Request-specific data
    typename POLICY::ResponseDataType _responseData;
};

typedef StatusRequest<StatusReplicationRequestPolicy> StatusReplicationRequest;
typedef StatusRequest<StatusDeleteRequestPolicy> StatusDeleteRequest;
typedef StatusRequest<StatusFindRequestPolicy> StatusFindRequest;
typedef StatusRequest<StatusFindAllRequestPolicy> StatusFindAllRequest;
typedef StatusRequest<StatusEchoRequestPolicy> StatusEchoRequest;
typedef StatusRequest<StatusDirectorIndexRequestPolicy> StatusDirectorIndexRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlQueryRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlCreateDbRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlDeleteDbRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlEnableDbRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlDisableDbRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlGrantAccessRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlCreateTableRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlDeleteTableRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlRemoveTablePartitionsRequest;
typedef StatusRequest<StatusSqlRequestPolicy> StatusSqlDeleteTablePartitionRequest;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_STATUSREQUEST_H
