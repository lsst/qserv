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
#ifndef LSST_QSERV_REPLICA_CLUSTERHEALTHJOB_H
#define LSST_QSERV_REPLICA_CLUSTERHEALTHJOB_H

// System headers
#include <functional>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Job.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/TestEchoQservMgtRequest.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class ClusterHealth captures the summary status of services within a cluster
 * as reported by the job upon its completion.
 */
class ClusterHealth {
public:
    /**
     * Normal (and the only) constructor requires to know names of all workers
     * contributing into the report.
     * @param workers  The names of the workers.
     */
    ClusterHealth(std::vector<std::string> const& workers);

    ClusterHealth() = delete;
    ClusterHealth(ClusterHealth const&) = delete;
    ClusterHealth& operator=(ClusterHealth const&) = delete;

    ~ClusterHealth() = default;

    /// @return  A value of 'true' if all services are up.
    bool good() const { return _good; }

    /// @return  The status of the Replication worker services ('true' if responded).
    std::map<std::string, bool> const& replication() const { return _replication; }

    /// @return  The status of the Qserv worker services ('true' if responded).
    std::map<std::string, bool> const& qserv() const { return _qserv; }

    /**
     * Update a state of a Replication agent worker and recompute the summary state.
     * @param worker  The name of a worker.
     * @param state  The new state of the worker.
     */
    void updateReplicationState(std::string const& worker, bool state);

    /**
     * Update a state of a Qserv worker and recompute the summary state.
     * @param worker  The name of a worker.
     * @param state  The new state of the worker.
     */
    void updateQservState(std::string const& worker, bool state);

private:
    /**
     * Recompute and update the summary state (data member 'good') of the object.
     */
    void _updateSummaryState();

private:
    /// A value of 'true' if all services are up.
    bool _good;

    /// Status of the Replication worker services ('true' if responded).
    std::map<std::string, bool> _replication;

    /// Status of the Qserv worker services ('true' if responded).
    std::map<std::string, bool> _qserv;
};

/**
 * Class ClusterHealthJob represents a tool which will send probes to the Replication
 * worker services and Qserv (if enabled) services of all worker nodes. Upon its
 * completion the job will report a status of each service.
 *
 * The job is implemented not to have any side effects on either class of services.
 */
class ClusterHealthJob : public Job {
public:
    typedef std::shared_ptr<ClusterHealthJob> Ptr;

    /// The function type for notifications on the completion of the request.
    typedef std::function<void(Ptr)> CallbackType;

    /// @return  The unique name distinguishing this class from other types of jobs.
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param timeoutSec  The maximum number of seconds that (all) requests are allowed
     *   to wait before finish or expire. If the parameter is set to 0 then
     *   the corresponding timeout (for requests) from the Configuration service
     *   will be assumed. ARTTENTION: this timeout could be quite lengthy.
     * @param allWorkers  The flag that if 'true' then send probes to all workers,
     *   otherwise the enabled workers will be considered only.
     * @param controller  The service for launching requests.
     * @param parentJobId  An identifier of the parent job.
     * @param onFinish  A callback function to be called upon a completion of the job.
     * @param priority  The priority of the job.
     * @return  A pointer to the created object.
     */
    static Ptr create(unsigned int timeoutSec, bool allWorkers, Controller::Ptr const& controller,
                      std::string const& parentJobId, CallbackType const& onFinish, int priority);

    ClusterHealthJob() = delete;
    ClusterHealthJob(ClusterHealthJob const&) = delete;
    ClusterHealthJob& operator=(ClusterHealthJob const&) = delete;

    ~ClusterHealthJob() final = default;

    /// @return  The maximum number of seconds that (all) requests are allowed to wait
    /// before finish or expire.
    unsigned int timeoutSec() const { return _timeoutSec; }

    /// @return  A value of 'true' if the job probes all known workers.
    bool allWorkers() const { return _allWorkers; }

    /**
     * @return  The cluster summary report.
     * @throw std::logic_error  If the method is called before the job finishes.
     */
    ClusterHealth const& clusterHealth() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

private:
    /// @see ClusterHealthJob::create()
    ClusterHealthJob(unsigned int timeoutSec, bool allWorkers, Controller::Ptr const& controller,
                     std::string const& parentJobId, CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the Replication worker probes.
     * @param request  A pointer to a request.
     */
    void _onRequestFinish(ServiceStatusRequest::Ptr const& request);

    /**
     * The callback function to be invoked on a completion of the Qserv worker probes.
     * @param request  A pointer to a request.
     */
    void _onRequestFinish(TestEchoQservMgtRequest::Ptr const& request);

    // Input parameters

    unsigned int const _timeoutSec;
    bool const _allWorkers;
    CallbackType _onFinish;

    /// Requests sent to the Replication workers registered by their identifiers
    std::map<std::string, ServiceStatusRequest::Ptr> _requests;

    /// Requests sent to the Qserv workers registered by their identifiers
    std::map<std::string, TestEchoQservMgtRequest::Ptr> _qservRequests;

    /// Result to be returned
    ClusterHealth _health;

    /// The number of started requests
    size_t _numStarted = 0;

    /// The number of finished requests
    size_t _numFinished = 0;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CLUSTERHEALTHJOB_H
