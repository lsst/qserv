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
#include <atomic>
#include <functional>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Job.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/TestEchoQservMgtRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * This data structure represents a summary of services within a cluster
 * s reported by the job upon its completion.
 */
class ClusterHealth {

public:

    /**
     * Normal (and the only) constructor requires to know names
     * of all workers contributing into the report.
     *
     * @param workers - names of the workers
     */
    ClusterHealth(std::vector<std::string> const& workers);

    // Default construction and copy semantics are prohibited

    ClusterHealth() = delete;
    ClusterHealth(ClusterHealth const&) = delete;
    ClusterHealth& operator=(ClusterHealth const&) = delete;

    ~ClusterHealth() = default;

    /// @return 'true' if all services are up
    bool good() const { return _good; }

    /// @return status of the Replication worker services ('true' if responded)
    std::map<std::string, bool> const& replication() const { return _replication; }

    /// @return status of the Qserv worker services ('true' if responded)
    std::map<std::string, bool> const& qserv() const { return _qserv; }

    /**
     * Update a state of a Replication agent worker and recompute the summary state
     *
     * @param worker - the name of a worker
     * @param state  - new state of the worker
     */
    void updateReplicationState(std::string const& worker,
                                bool state);

    /**
     * Update a state of a Qserv worker and recompute the summary state
     *
     * @param worker - the name of a worker
     * @param state  - new state of the worker
     */
    void updateQservState(std::string const& worker,
                          bool state);

private:

    /**
     * Recompute and update the summary state (data member 'good')
     * of the object.
     */
    void _updateSummaryState();

private:

    /// 'true' if all services are up
    bool _good;

    /// Status of the Replication worker services ('true' if responded)
    std::map<std::string, bool> _replication;

    /// Status of the Qserv worker services ('true' if responded)
    std::map<std::string, bool> _qserv;
};

/**
  * Class ClusterHealthJob represents a tool which will send probes to the Replication
  * worker services and Qserv (if enabled) services of all worker nodes. Upon its
  * completion the job will report a status of each service.
  *
  * The job is implemented not to have any side effects on either class of services.
  */
class ClusterHealthJob : public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ClusterHealthJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param timeoutSec
     *   maximum number of seconds that (all) requests are allowed to wait
     *   before finish or expire. If the parameter is set to 0 then
     *   the corresponding timeout (for requests) from the Configuration service
     *   will be assumed. ARTTENTION: this timeout could be quite lengthy.
     *
     * @param allWorkers
     *   if 'true' then send probes to all workers, otherwise the enabled workers
     *   will be considered only
     *
     * @param controller
     *   for launching requests
     *
     * @param parentJobId
     *   (optional) identifier of a parent job
     *
     * @param onFinish
     *   (optional) callback function to be called upon a completion of the job
     *
     * @param options
     *   (optional) job options
     *
     * @return pointer to the created object
     */
    static Ptr create(unsigned int timeoutSec,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    ClusterHealthJob() = delete;
    ClusterHealthJob(ClusterHealthJob const&) = delete;
    ClusterHealthJob& operator=(ClusterHealthJob const&) = delete;

    ~ClusterHealthJob() final = default;

    /// @return maximum number of seconds that (all) requests are allowed to wait
    /// before finish or expire
    unsigned int timeoutSec() const { return _timeoutSec; }

    /// @return 'true' if the job probes all known workers
    bool allWorkers() const { return _allWorkers; }

    /**
     * @return summary report
     *
     * @throw std::logic_error if the method is called before the job finishes
     */
    ClusterHealth const& clusterHealth() const;

    /**
     * @see Job::extendedPersistentState()
     */
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

    /**
     * @see Job::persistentLogData()
     */
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see ClusterHealthJob::create()
     */
    ClusterHealthJob(unsigned int timeoutSec,
                     bool allWorkers,
                     Controller::Ptr const& controller,
                     std::string const& parentJobId,
                     CallbackType const& onFinish,
                     Job::Options const& options);

    /**
      * @see Job::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * @see Job::startImpl()
      */
    void cancelImpl(util::Lock const& lock) final;

    /**
      * @see Job::notify()
      */
    void notify(util::Lock const& lock) final;

    /**
     * The callback function to be invoked on a completion of the Replication
     * worker probes.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(ServiceStatusRequest::Ptr const& request);

    /**
     * The callback function to be invoked on a completion of the Qserv
     * worker probes.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(TestEchoQservMgtRequest::Ptr const& request);

protected:

    /// The maximum number life span (seconds) of requests
    unsigned int const _timeoutSec;

    /// The worker selector
    bool _allWorkers;

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// Requests sent to the Replication workers registered by their identifiers
    std::map<std::string, ServiceStatusRequest::Ptr> _requests;

    /// Requests sent to the Qserv workers registered by their identifiers
    std::map<std::string, TestEchoQservMgtRequest::Ptr> _qservRequests;
    
    /// Result to be returned
    ClusterHealth _health;

    /// The number of started requests
    std::atomic<size_t> _numStarted;

    /// The number of finished requests
    std::atomic<size_t> _numFinished;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CLUSTERHEALTHJOB_H
