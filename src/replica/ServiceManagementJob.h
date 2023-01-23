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
#ifndef LSST_QSERV_REPLICA_SERVICEMANAGEMENTJOB_H
#define LSST_QSERV_REPLICA_SERVICEMANAGEMENTJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/Job.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure ServiceManagementJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct ServiceManagementJobResult {
    /// Result sets for each worker
    std::map<std::string, ServiceState> serviceState;

    /// Per-worker flags indicating which workers responded to the requests
    std::map<std::string, bool> workers;
};

/**
 * Class ServiceManagementBaseJob is an intermediate base class for a family of requests
 * managing worker services. The request will be broadcast to all workers of a setup.
 * Results will be collected into the above defined data structure.
 */
class ServiceManagementBaseJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementBaseJob> Ptr;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    // Default construction and copy semantics are prohibited

    ServiceManagementBaseJob() = delete;
    ServiceManagementBaseJob(ServiceManagementBaseJob const&) = delete;
    ServiceManagementBaseJob& operator=(ServiceManagementBaseJob const&) = delete;

    ~ServiceManagementBaseJob() override = default;

    // Trivial get methods

    bool allWorkers() const { return _allWorkers; }

    unsigned int requestExpirationIvalSec() const { return _requestExpirationIvalSec; }

    /**
     * Return the combined result of the operation
     *
     * @note:
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throws std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    ServiceManagementJobResult const& getResultData() const;

protected:
    /// @see Job::startImpl()
    void startImpl(replica::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(replica::Lock const& lock) final;

    /**
     * Normal constructor.
     *
     * @param requestName
     *   the name of a specific request (as defined by a subclass)
     *
     * @param allWorkers
     *   engage all known workers regardless of their status. If the flag
     *   is set to 'false' then only 'ENABLED' workers which are not in
     *   the 'READ-ONLY' state will be involved into the operation.
     *
     * @param requestExpirationIvalSec
     *   the number of seconds before the requests will be declared as expired
     *   unless receiving responses from them.
     *
     * @param controller
     *   is needed launching requests and accessing the Configuration
     *
     * @param parentJobId
     *   an identifier of a parent job
     *
     * @param priority
     *   defines the job priority
     */
    ServiceManagementBaseJob(std::string const& requestName, bool allWorkers,
                             unsigned int requestExpirationIvalSec, Controller::Ptr const& controller,
                             std::string const& parentJobId, int priority);

    /**
     * Submit type-specific request
     *
     * @param worker  the name of a worker to which the request will be sent
     * @return a newly created & submitted object
     */
    virtual ServiceManagementRequestBase::Ptr submitRequest(std::string const& worker) = 0;

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void onRequestFinish(ServiceManagementRequestBase::Ptr const& request);

private:
    // Input parameters

    bool const _allWorkers;
    unsigned int _requestExpirationIvalSec;

    /// A collection of requests implementing the operation
    std::list<ServiceManagementRequestBase::Ptr> _requests;

    /// This counter is used for tracking a condition for completing the job
    /// before computing its final state.
    size_t _numFinished = 0;

    /// The result of the operation (gets updated as requests are finishing)
    ServiceManagementJobResult _resultData;
};

/**
 * Class ServiceManagementJob is a generic implementation for a family of requests
 * managing worker services. Types of the requests are specified via the template
 * parameter of the class. The request will be broadcast to all workers of a setup.
 */
template <class REQUEST>
class ServiceManagementJob : public ServiceManagementBaseJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementJob<REQUEST>> Ptr;

    /// The function type for notifications on the completion of the job
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName() {
        return "ServiceManagementJob[" + std::string(REQUEST::Policy::requestName()) + "]";
    }

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @see ServiceManagementBaseJob::ServiceManagementBaseJob()
     */
    static Ptr create(bool allWorkers, unsigned int requestExpirationIvalSec,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority) {
        return Ptr(new ServiceManagementJob(allWorkers, requestExpirationIvalSec, controller, parentJobId,
                                            onFinish, priority));
    }

    // Default construction and copy semantics are prohibited

    ServiceManagementJob() = delete;
    ServiceManagementJob(ServiceManagementJob const&) = delete;
    ServiceManagementJob& operator=(ServiceManagementJob const&) = delete;

    ~ServiceManagementJob() final = default;

protected:
    void notify(replica::Lock const& lock) final {
        notifyDefaultImpl<ServiceManagementJob<REQUEST>>(lock, _onFinish);
    }

    /// @see ServiceManagementBaseJob::submitRequest()
    ServiceManagementRequestBase::Ptr submitRequest(std::string const& worker) final {
        auto const self = shared_from_base<ServiceManagementJob<REQUEST>>();
        return controller()->template workerServiceRequest<REQUEST>(
                worker, [self](typename REQUEST::Ptr const& ptr) { self->onRequestFinish(ptr); }, priority(),
                id(), requestExpirationIvalSec());
    }

private:
    /// @see ServiceManagementJob::create())
    ServiceManagementJob(bool allWorkers, unsigned int requestExpirationIvalSec,
                         Controller::Ptr const& controller, std::string const& parentJobId,
                         CallbackType const& onFinish, int priority)
            : ServiceManagementBaseJob(REQUEST::Policy::requestName(), allWorkers, requestExpirationIvalSec,
                                       controller, parentJobId, priority),
              _onFinish(onFinish) {}

    // Input parameters

    CallbackType _onFinish;  /// @note is reset when the job finishes
};

typedef ServiceManagementJob<ServiceStatusRequest> ServiceStatusJob;
typedef ServiceManagementJob<ServiceRequestsRequest> ServiceRequestsJob;
typedef ServiceManagementJob<ServiceSuspendRequest> ServiceSuspendJob;
typedef ServiceManagementJob<ServiceResumeRequest> ServiceResumeJob;
typedef ServiceManagementJob<ServiceDrainRequest> ServiceDrainJob;
typedef ServiceManagementJob<ServiceReconfigRequest> ServiceReconfigJob;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SERVICEMANAGEMENTJOB_H
