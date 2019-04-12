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
#ifndef LSST_QSERV_REPLICA_QSERVSTATUSJOB_H
#define LSST_QSERV_REPLICA_QSERVSTATUSJOB_H

// System headers
#include <atomic>
#include <functional>
#include <map>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Job.h"
#include "replica/GetStatusQservMgtRequest.h"
#include "replica/ServiceManagementRequest.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * This data structure represents status responses from Qserv workers
 * reported by the job upon its completion.
 */
struct QservStatus {

    /// key: worker name, value: 'true' if got a response from the worker
    std::map<std::string, bool> workers;

    /// Key: worker, value: status info received from the worker
    std::map<std::string, nlohmann::json> info;
};

/**
 * Class QservStatusJob represents a tool which will obtain various
 * info on the on-going status of the Qserv workers. Upon its
 * completion the job will report a status of each service.
 *
 * The job is implemented not to have any side effects Qserv workers.
 */
class QservStatusJob : public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservStatusJob> Ptr;

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
     * @return
     *   pointer to the created object
     */
    static Ptr create(unsigned int timeoutSec,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    QservStatusJob() = delete;
    QservStatusJob(QservStatusJob const&) = delete;
    QservStatusJob& operator=(QservStatusJob const&) = delete;

    ~QservStatusJob() final = default;

    /// @return maximum number of seconds that (all) requests are allowed to wait
    /// before finish or expire
    unsigned int timeoutSec() const { return _timeoutSec; }

    /// @return 'true' if the job probes all known workers
    bool allWorkers() const { return _allWorkers; }

    /**
     * @return
     *   status report from workers
     *
     * @throw std::logic_error
     *   if the method is called before the job finishes
     */
    QservStatus const& qservStatus() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    void startImpl(util::Lock const& lock) final;

    void cancelImpl(util::Lock const& lock) final;

    void notify(util::Lock const& lock) final;

private:

    /// @see QservStatusJob::create()
    QservStatusJob(unsigned int timeoutSec,
                     bool allWorkers,
                     Controller::Ptr const& controller,
                     std::string const& parentJobId,
                     CallbackType const& onFinish,
                     Job::Options const& options);

    /**
     * The callback function to be invoked on a completion of the Qserv
     * worker requests.
     *
     * @param request
     *   a pointer to a request
     */
    void _onRequestFinish(GetStatusQservMgtRequest::Ptr const& request);

    // Input parameters

    unsigned int const _timeoutSec;
    bool         const _allWorkers;
    CallbackType       _onFinish;

    /// Requests sent to the Qserv workers registered by their identifiers
    std::map<std::string, GetStatusQservMgtRequest::Ptr> _requests;
    
    /// Result to be returned
    QservStatus _qservStatus;

    /// The number of started requests
    std::atomic<size_t> _numStarted;

    /// The number of finished requests
    std::atomic<size_t> _numFinished;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_QSERVSTATUSJOB_H
