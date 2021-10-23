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
#ifndef LSST_QSERV_REPLICA_STOPREQUEST_H
#define LSST_QSERV_REPLICA_STOPREQUEST_H

/**
 * This header declares a collection of classes representing various
 * server-side request cancellation tools (requests) as a part of the
 * Controller-side Replication Framework.
 *
 * @see class StopRequestReplicate
 * @see class StopRequestDelete
 * @see class StopRequestFind
 * @see class StopRequestFindAll
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
#include "replica/StopRequestBase.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class IndexInfo;
}}} // namespace lsst::qserv::replica

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

class StopReplicationRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseReplicate;
    using ResponseDataType        = ReplicaInfo;
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

class StopDeleteRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseDelete;
    using ResponseDataType        = ReplicaInfo;
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

class StopFindRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseFind;
    using ResponseDataType        = ReplicaInfo;
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

class StopFindAllRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseFindAll;
    using ResponseDataType        = ReplicaInfoCollection;
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

class StopEchoRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseEcho;
    using ResponseDataType        = std::string;
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

class StopIndexRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseIndex;
    using ResponseDataType        = IndexInfo;
    using TargetRequestParamsType = IndexRequestParams;

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

class StopSqlRequestPolicy {
public:
    using ResponseMessageType     = ProtocolResponseSql;
    using ResponseDataType        = SqlResultSet;
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
  * Generic class StopRequest extends its base class to allow further policy-based
  * customization of specific requests.
  */
template <typename POLICY>
class StopRequest: public StopRequestBase {
public:
    typedef std::shared_ptr<StopRequest<POLICY>> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    StopRequest() = delete;
    StopRequest(StopRequest const&) = delete;
    StopRequest &operator=(StopRequest const&) = delete;

    ~StopRequest() final = default;

    /// @return target request specific parameters
    typename POLICY::TargetRequestParamsType const& targetRequestParams() const {
        return _targetRequestParams;
    }

    /**
     * @return The request-specific extended data reported upon a successful
     *   completion of the request.
     */
    typename POLICY::ResponseDataType const& responseData() const {
        return _responseData;
    }

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
     * @return  A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& targetRequestId,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger) {
        return StopRequest<POLICY>::Ptr(new StopRequest<POLICY>(
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
    void notify(util::Lock const& lock) final {
        notifyDefaultImpl<StopRequest<POLICY>>(lock, _onFinish);
    }

    void send(util::Lock const& lock) final {
        auto self = shared_from_base<StopRequest<POLICY>>();
        messenger()->send<typename POLICY::ResponseMessageType>(
                worker(), id(), priority(), buffer(),
                [self] (std::string const& id, bool success,
                        typename POLICY::ResponseMessageType const& response) {
                    if (success) self->analyze(true, self->_parseResponse(response));
                    else self->analyze(false);
                }
        );
    }

    void saveReplicaInfo() final {
        auto const self = shared_from_base<StopRequest<POLICY>>();
        POLICY::saveReplicaInfo(self);
    }

private:
    StopRequest(ServiceProvider::Ptr const& serviceProvider,
                boost::asio::io_service& io_service,
                char const* requestName,
                std::string const& worker,
                std::string const& targetRequestId,
                ProtocolQueuedRequestType targetRequestType,
                CallbackType const& onFinish,
                int priority,
                bool keepTracking,
                std::shared_ptr<Messenger> const& messenger)
        :   StopRequestBase(serviceProvider, io_service, requestName, worker, targetRequestId,
                            targetRequestType, priority, keepTracking, messenger),
            _onFinish(onFinish) {
    }

    /**
     * Parse request-specific reply.
     * @param message  A message to be parsed.
     * @return  The status of the operation reported by a server.
     */
    ProtocolStatus _parseResponse(typename POLICY::ResponseMessageType const& message) {

        // This lock must be acquired because the method is going to modify
        // results of the request. Note that the operation doesn't care
        // about the global state of the request (wether it's already finished
        // or not).
        util::Lock lock(_mtx, context() + __func__);

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

typedef StopRequest<StopReplicationRequestPolicy> StopReplicationRequest;
typedef StopRequest<StopDeleteRequestPolicy>      StopDeleteRequest;
typedef StopRequest<StopFindRequestPolicy>        StopFindRequest;
typedef StopRequest<StopFindAllRequestPolicy>     StopFindAllRequest;
typedef StopRequest<StopEchoRequestPolicy>        StopEchoRequest;
typedef StopRequest<StopIndexRequestPolicy>       StopIndexRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlQueryRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlCreateDbRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlDeleteDbRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlEnableDbRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlDisableDbRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlGrantAccessRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlCreateTableRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlDeleteTableRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlRemoveTablePartitionsRequest;
typedef StopRequest<StopSqlRequestPolicy>         StopSqlDeleteTablePartitionRequest;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STOPREQUEST_H
