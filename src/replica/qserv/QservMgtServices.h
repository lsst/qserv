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
#ifndef LSST_QSERV_REPLICA_QSERVMGTSERVICES_H
#define LSST_QSERV_REPLICA_QSERVMGTSERVICES_H

// System headers
#include <map>
#include <memory>
#include <vector>

// Qserv headers
#include "global/intTypes.h"
#include "replica/qserv/AddReplicaQservMgtRequest.h"
#include "replica/qserv/GetReplicasQservMgtRequest.h"
#include "replica/qserv/GetDbStatusQservMgtRequest.h"
#include "replica/qserv/GetConfigQservCzarMgtRequest.h"
#include "replica/qserv/GetConfigQservMgtRequest.h"
#include "replica/qserv/GetQueryProgressQservCzarMgtRequest.h"
#include "replica/qserv/GetResultFilesQservMgtRequest.h"
#include "replica/qserv/GetStatusQservCzarMgtRequest.h"
#include "replica/qserv/GetStatusQservMgtRequest.h"
#include "replica/qserv/RemoveReplicaQservMgtRequest.h"
#include "replica/qserv/SetReplicasQservMgtRequest.h"
#include "replica/qserv/TestEchoQservMgtRequest.h"
#include "replica/util/Mutex.h"
#include "wbase/TaskState.h"

// Forward declarations
namespace lsst::qserv::wbase {
class TaskSelector;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {
namespace detail {

/**
 * Class QservMgtRequestWrapper is an abstract base for implementing requests
 * registry as a polymorphic collection to store active requests. Pure virtual
 * methods of the class will be overridden by request-type-specific implementations
 * (see structure RequestWrappeImplr<REQUEST_TYPE> in the .cc file) capturing
 * type-dependent pointer and a callback function.
 */
class QservMgtRequestWrapper {
public:
    typedef std::shared_ptr<QservMgtRequestWrapper> Ptr;

    QservMgtRequestWrapper() = default;
    QservMgtRequestWrapper(QservMgtRequestWrapper const&) = delete;
    QservMgtRequestWrapper& operator=(QservMgtRequestWrapper const&) = delete;

    virtual ~QservMgtRequestWrapper() = default;

    /**
     * This method (to be implemented by subclasses) will be called upon
     * a completion of a request to notify a subscriber on the event.
     */
    virtual void notify() const = 0;

    /// @return a pointer to the stored request object
    virtual std::shared_ptr<QservMgtRequest> request() const = 0;
};

/**
 * Class QservMgtRequestWrapperImpl represents request-type specific wrappers.
 */
template <class T>
class QservMgtRequestWrapperImpl : public QservMgtRequestWrapper {
public:
    QservMgtRequestWrapperImpl() = delete;
    QservMgtRequestWrapperImpl(QservMgtRequestWrapperImpl const&) = delete;
    QservMgtRequestWrapperImpl& operator=(QservMgtRequestWrapperImpl const&) = delete;

    QservMgtRequestWrapperImpl(typename T::Ptr const& request, typename T::CallbackType const& onFinish)
            : QservMgtRequestWrapper(), _request(request), _onFinish(onFinish) {}

    virtual ~QservMgtRequestWrapperImpl() final = default;

    /// The implementation of the virtual method defined in the base class
    virtual void notify() const final {
        if (nullptr != _onFinish) {
            // Clearing the stored callback after finishing the up-stream notification
            // has two purposes:
            // 1. it guaranties (exactly) one time notification
            // 2. it breaks the up-stream dependency on a caller object if a shared
            //    pointer to the object was mentioned as the lambda-function's closure
            auto onFinish = std::move(_onFinish);
            _onFinish = nullptr;
            onFinish(_request);
        }
    }

    /// Implement a virtual method of the base class
    virtual std::shared_ptr<QservMgtRequest> request() const final { return _request; }

private:
    typename T::Ptr _request;
    mutable typename T::CallbackType _onFinish;
};
}  // namespace detail

/**
 * Class QservMgtServices is a high-level interface to the Qserv management
 * services used by the replication system.
 */
class QservMgtServices : public std::enable_shared_from_this<QservMgtServices> {
public:
    typedef std::shared_ptr<QservMgtServices> Ptr;

    /**
     * The factory method for instantiating a proper service object based
     * on an application configuration.
     * @param serviceProvider Is required for accessing configuration parameters.
     * @return A pointer to the created object.
     */
    static Ptr create(std::shared_ptr<ServiceProvider> const& serviceProvider);

    QservMgtServices() = delete;
    QservMgtServices(QservMgtServices const&) = delete;
    QservMgtServices& operator=(QservMgtServices const&) = delete;

    ~QservMgtServices() = default;

    /// @return reference to the ServiceProvider object
    std::shared_ptr<ServiceProvider> const& serviceProvider() const { return _serviceProvider; }

    /**
     * Notify Qserv worker on availability of a new replica
     * @param chunk  The chunk whose replica will be enabled on the Qserv worker.
     * @param databases The names of databases.
     * @param worker  The name of a worker where the replica is residing.
     * @param onFinish  A callback function called on a completion of the operation.
     * @param jobId  An optional identifier of a job specifying a context
     *   in which a request will be executed.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    AddReplicaQservMgtRequest::Ptr addReplica(
            unsigned int chunk, std::vector<std::string> const& databases, std::string const& worker,
            AddReplicaQservMgtRequest::CallbackType const& onFinish = nullptr, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Notify Qserv worker on a removal of a replica
     * @param chunk  The chunk whose replicas will be disabled at the Qserv worker.
     * @param databases  The names of databases.
     * @param worker  The name of a worker where the replica is residing.
     * @param force  A flag to tell Qserv that the replica has to be removed from its
     *   repository regardless if there are any outstanding requests using the replica.
     * @param onFinish  A callback function called on a completion of the operation.
     * @param jobId  An optional identifier of a job specifying a context
     *   in which a request will be executed.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    RemoveReplicaQservMgtRequest::Ptr removeReplica(
            unsigned int chunk, std::vector<std::string> const& databases, std::string const& worker,
            bool force, RemoveReplicaQservMgtRequest::CallbackType const& onFinish = nullptr,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    /**
     * Fetch replicas known to a Qserv worker
     * @param databaseFamily  The name of a database family.
     * @param worker  The name of a worker.
     * @param inUseOnly  A flag telling the method to return replicas which are
     *   presently in use
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetReplicasQservMgtRequest::Ptr getReplicas(
            std::string const& databaseFamily, std::string const& worker, bool inUseOnly = false,
            std::string const& jobId = "", GetReplicasQservMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Enable a collection of replicas at a Qserv worker
     * @param worker  The name of a worker.
     * @param newReplicas  A collection of new replicas (NOTE: useCount field is ignored).
     * @param databases  The names of databases to be affected by the request,
     * @param force   A flag telling the metod to proceed with the operation even if some
     *   replicas affected by the operation are still in use.
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    SetReplicasQservMgtRequest::Ptr setReplicas(
            std::string const& worker, QservReplicaCollection const& newReplicas,
            std::vector<std::string> const& databases, bool force = false, std::string const& jobId = "",
            SetReplicasQservMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Send a data string to a Qserv worker and get the same string in response
     * @param worker  The name of a worker.
     * @param data  The data string to be sent to a worker.
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    TestEchoQservMgtRequest::Ptr echo(std::string const& worker, std::string const& data,
                                      std::string const& jobId = "",
                                      TestEchoQservMgtRequest::CallbackType const& onFinish = nullptr,
                                      unsigned int requestExpirationIvalSec = 0);

    /**
     * Request detailed status of a Qserv worker
     * @param worker  The name of a worker.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param taskSelector An optional task selection criterias.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetStatusQservMgtRequest::Ptr status(std::string const& worker, std::string const& jobId = "",
                                         wbase::TaskSelector const& taskSelector = wbase::TaskSelector(),
                                         GetStatusQservMgtRequest::CallbackType const& onFinish = nullptr,
                                         unsigned int requestExpirationIvalSec = 0);

    /**
     * Request detailed status on the database service of a Qserv worker
     * @param worker  The name of a worker.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetDbStatusQservMgtRequest::Ptr databaseStatus(
            std::string const& worker, std::string const& jobId = "",
            GetDbStatusQservMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Request configuration parameters of a Qserv worker
     * @param worker  The name of a worker.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetConfigQservMgtRequest::Ptr config(std::string const& worker, std::string const& jobId = "",
                                         GetConfigQservMgtRequest::CallbackType const& onFinish = nullptr,
                                         unsigned int requestExpirationIvalSec = 0);

    /**
     * Request info on the partial result files of a Qserv worker
     * @param worker  The name of a worker.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param queryIds The optional selector for queries. If empty then all queries will
     *   be considered.
     * @param maxFiles The optional limit for maximum number of files to be reported.
     *   If 0 then no limit is set.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetResultFilesQservMgtRequest::Ptr resultFiles(
            std::string const& worker, std::string const& jobId = "",
            std::vector<QueryId> const& queryIds = std::vector<QueryId>(), unsigned int maxFiles = 0,
            GetResultFilesQservMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Request detailed status of a Qserv Czar
     * @param czarName  The name of a Czar.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetStatusQservCzarMgtRequest::Ptr czarStatus(
            std::string const& czarName, std::string const& jobId = "",
            GetStatusQservCzarMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Request the query progress records from the Qserv Czar.
     * @param czarName  The name of a Czar.
     * @param jobId  An optional identifier of a job specifying a context in which
     *   a request will be executed.
     * @param queryIds The optional selector for queries. If empty then all queries will
     *   be considered.
     * @param lastSeconds The optional limit for age of the queries. If 0 then no limit is set.
     * @param queryStatus The optional status ("EXECUTING", "COMPLETED", "FAILED", etc. or
     *   the empty string for all) of the queries to be selected.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetQueryProgressQservCzarMgtRequest::Ptr czarQueryProgress(
            std::string const& czarName, std::string const& jobId = "",
            std::vector<QueryId> const& queryIds = std::vector<QueryId>(), unsigned int lastSeconds = 0,
            std::string const& queryStatus = std::string(),
            GetQueryProgressQservCzarMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Request configuration parameters of a Qserv Czar
     * @param czarName  The name of a Czar.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param onFinish  A callback function to be called upon request completion.
     * @param requestExpirationIvalSec The maximum amount of time to wait before
     *   completion of the request. If a value of the parameter is set to 0 then no
     *   limit will be enforced.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetConfigQservCzarMgtRequest::Ptr czarConfig(
            std::string const& czarName, std::string const& jobId = "",
            GetConfigQservCzarMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

private:
    /**
     * @param serviceProvider Is required for accessing configuration parameters.
     */
    explicit QservMgtServices(std::shared_ptr<ServiceProvider> const& serviceProvider);

    /**
     * Register the request (along with its callback) by its unique id in the local registry.
     * When the request will finish it'll be automatically removed from the registry.
     * @param func The name of a calling context (for error reporting and logging).
     * @param request A request to be registered.
     * @param onFinish A client callback to be called upon completion of the reqeust.
     */
    template <typename T>
    void _register(std::string const& func, std::shared_ptr<T> const& request,
                   typename T::CallbackType const& onFinish) {
        replica::Lock const lock(_mtx, "QservMgtServices::" + func);
        _registry[request->id()] = std::make_shared<detail::QservMgtRequestWrapperImpl<T>>(request, onFinish);
    }

    /**
     * Finalize the completion of the request.
     * This method will notify a requester on the completion of the operation and it will
     * also remove the request from the server's registry.
     * @param id  A unique identifier of a completed request.
     */
    void _finish(std::string const& id);

    // Input parameters

    std::shared_ptr<ServiceProvider> const _serviceProvider;

    /// The registry for the on-going requests.
    std::map<std::string, std::shared_ptr<detail::QservMgtRequestWrapper>> _registry;

    /// The mutex for enforcing thread safety of the class's public API and internal operations.
    mutable replica::Mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVMGTSERVICES_H
