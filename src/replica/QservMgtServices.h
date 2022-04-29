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
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/GetReplicasQservMgtRequest.h"
#include "replica/GetStatusQservMgtRequest.h"
#include "replica/RemoveReplicaQservMgtRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SetReplicasQservMgtRequest.h"
#include "replica/TestEchoQservMgtRequest.h"
#include "util/Mutex.h"

// Forward declarations
class XrdSsiService;

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * For exceptions thrown on unrecoverable connection failures to XRootD/SSI services.
 */
class QservMgtConnectionError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Structure QservMgtRequestWrapper is an abstract base for implementing requests
 * registry as a polymorphic collection to store active requests. Pure virtual
 * methods of the class will be overridden by request-type-specific implementations
 * (see structure RequestWrappeImplr<REQUEST_TYPE> in the .cc file) capturing
 * type-dependent pointer and a callback function.
 */
struct QservMgtRequestWrapper {
    typedef std::shared_ptr<QservMgtRequestWrapper> Ptr;

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
 * Class QservMgtServices is a high-level interface to the Qserv management
 * services used by the replication system.
 */
class QservMgtServices : public std::enable_shared_from_this<QservMgtServices> {
public:
    typedef std::shared_ptr<QservMgtServices> Ptr;

    /**
     * The factory method for instantiating a proper service object based
     * on an application configuration.
     *
     * @param serviceProvider Is required for accessing configuration parameters.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    QservMgtServices() = delete;
    QservMgtServices(QservMgtServices const&) = delete;
    QservMgtServices& operator=(QservMgtServices const&) = delete;

    ~QservMgtServices() = default;

    /// @return reference to the ServiceProvider object
    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }

    /**
     * Notify Qserv worker on availability of a new replica
     *
     * @param chunk  The chunk whose replica will be enabled on the Qserv worker.
     * @param databases The names of databases.
     * @param worker  The name of a worker where the replica is residing.
     * @param onFinish  A callback function called on a completion of the operation.
     * @param jobId  An optional identifier of a job specifying a context
     *   in which a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0) allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     * @return A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    AddReplicaQservMgtRequest::Ptr addReplica(
            unsigned int chunk, std::vector<std::string> const& databases, std::string const& worker,
            AddReplicaQservMgtRequest::CallbackType const& onFinish = nullptr, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Notify Qserv worker on a removal of a replica
     *
     * @param chunk  The chunk whose replicas will be disabled at the Qserv worker.
     * @param databases  The names of databases.
     * @param worker  The name of a worker where the replica is residing.
     * @param force  A flag to tell Qserv that the replica has to be removed from its
     *   repository regardless if there are any outstanding requests using the replica.
     * @param onFinish  A callback function called on a completion of the operation.
     * @param jobId  An optional identifier of a job specifying a context
     *   in which a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0) allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    RemoveReplicaQservMgtRequest::Ptr removeReplica(
            unsigned int chunk, std::vector<std::string> const& databases, std::string const& worker,
            bool force, RemoveReplicaQservMgtRequest::CallbackType const& onFinish = nullptr,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);
    /**
     * Fetch replicas known to a Qserv worker
     *
     * @param databaseFamily  The name of a database family.
     * @param worker  The name of a worker.
     * @param inUseOnly  A flag telling the method to return replicas which are
     *   presently in use
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0)  allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetReplicasQservMgtRequest::Ptr getReplicas(
            std::string const& databaseFamily, std::string const& worker, bool inUseOnly = false,
            std::string const& jobId = "", GetReplicasQservMgtRequest::CallbackType const& onFinish = nullptr,
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Enable a collection of replicas at a Qserv worker
     *
     * @param worker  The name of a worker.
     * @param newReplicas  A collection of new replicas (NOTE: useCount field is ignored).
     * @param databases  The names of databases to be affected by the request,
     * @param force   A flag telling the metod to proceed with the operation even if some
     *   replicas affected by the operation are still in use.
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0)
     *   allowing to override the default value of the corresponding parameter from
     *   the Configuration.
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
     *
     * @param worker  The name of a worker.
     * @param data  The data string to be sent to a worker.
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0) allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    TestEchoQservMgtRequest::Ptr echo(std::string const& worker, std::string const& data,
                                      std::string const& jobId = "",
                                      TestEchoQservMgtRequest::CallbackType const& onFinish = nullptr,
                                      unsigned int requestExpirationIvalSec = 0);

    /**
     * Request detailed status of a Qserv worker
     *
     * @param worker  The name of a worker.
     * @param onFinish  A callback function to be called upon request completion.
     * @param jobId  An optional identifier of a job specifying a context in which
     *    a request will be executed.
     * @param requestExpirationIvalSec  An optional parameter (if differs from 0) allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     * @return  A pointer to the request object if the request was made. Return
     *   nullptr otherwise.
     */
    GetStatusQservMgtRequest::Ptr status(std::string const& worker, std::string const& jobId = "",
                                         GetStatusQservMgtRequest::CallbackType const& onFinish = nullptr,
                                         unsigned int requestExpirationIvalSec = 0);

private:
    /**
     * @param serviceProvider Is required for accessing configuration parameters.
     */
    explicit QservMgtServices(ServiceProvider::Ptr const& serviceProvider);

    /**
     * Finalize the completion of the request. This method will notify
     * a requester on the completion of the operation and it will also
     * remove the request from the server's registry.
     *
     * @param id  A unique identifier of a completed request.
     */
    void _finish(std::string const& id);

    /**
     * @return  A pointer to the XROOTD/SSI API service for launching worker management
     *   requests.
     * @throws QservMgtConnectionError If unable to establish a connection.
     */
    XrdSsiService* _xrdSsiService();

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;

    /// The cashed pointer to the XROOTD/SSI service gets initialized at a time
    /// the first request is being made.
    XrdSsiService* _service = nullptr;

    /// The registry of the on-going requests.
    std::map<std::string, std::shared_ptr<QservMgtRequestWrapper>> _registry;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable util::Mutex _mtx;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVMGTSERVICES_H
