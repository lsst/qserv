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
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/requests/ServiceManagementRequest.h"
#include "replica/util/Common.h"
#include "wbase/TaskState.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class GetStatusQservMgtRequest;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * This data structure represents status responses from Qserv workers
 * reported by the job upon its completion.
 */
struct QservStatus {
    /// key: worker name, value: 'true' if got a response from the worker
    std::map<std::string, bool> workers;

    /// Key: worker, value: status info received from the worker
    nlohmann::json info;
};

/**
 * Class QservStatusJob represents a tool which will obtain various
 * info on the on-going status of the Qserv workers. Upon its
 * completion the job will report a status of each service.
 *
 * The job is implemented not to have any side effects Qserv workers.
 */
class QservStatusJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservStatusJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<QservStatusJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Create the job.
     *
     * @param timeoutSec maximum number of seconds that (all) requests are allowed to wait
     *  before finish or expire. If the parameter is set to 0 then the corresponding default
     *  timeout (for requests) from the Config service will be assumed. Be aware that the default
     *  timeout could be quite lengthy. So, if the job is being launched for worker monitoring
     *  purposes from the time-constrained environment then it's better to be set to same
     *  reasonably small value.
     * @param allWorkers if 'true' then send probes to all workers, otherwise
     *  the enabled workers will be considered only
     * @param controller for launching requests
     * @param parentJobId (optional) identifier of a parent job
     * @param taskSelector (optional) task selection criterias
     * @param onFinish (optional) callback function to be called upon a completion of the job
     * @param priority (optional) priority level of the job
     * @return a pointer to the created object
     */
    static std::shared_ptr<QservStatusJob> create(
            unsigned int timeoutSec, bool allWorkers, std::shared_ptr<Controller> const& controller,
            std::string const& parentJobId = std::string(),
            wbase::TaskSelector const& taskSelector = wbase::TaskSelector(),
            CallbackType const& onFinish = nullptr, int priority = PRIORITY_NORMAL);

    QservStatusJob() = delete;
    QservStatusJob(QservStatusJob const&) = delete;
    QservStatusJob& operator=(QservStatusJob const&) = delete;

    virtual ~QservStatusJob() final = default;

    /// @return The actual value for the maximum number of seconds that (all) requests
    ///  are allowed to wait before finish or expire after a possible adjustment of
    ///  the parameter's value from the Config
    unsigned int timeoutSec() const { return _timeoutSec; }

    bool allWorkers() const { return _allWorkers; }
    wbase::TaskSelector const& taskSelector() const { return _taskSelector; }

    /**
     * @return status report from workers
     * @throw std::logic_error if the method is called before the job finishes
     */
    QservStatus const& qservStatus() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    QservStatusJob(unsigned int timeoutSec, bool allWorkers, std::shared_ptr<Controller> const& controller,
                   std::string const& parentJobId, wbase::TaskSelector const& taskSelector,
                   CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the Qserv worker requests.
     * @param request a pointer to a request
     */
    void _onRequestFinish(std::shared_ptr<GetStatusQservMgtRequest> const& request);

    // Input parameters

    unsigned int const _timeoutSec;
    bool const _allWorkers;
    wbase::TaskSelector const _taskSelector;
    CallbackType _onFinish;

    /// Requests sent to the Qserv workers registered by their identifiers
    std::map<std::string, std::shared_ptr<GetStatusQservMgtRequest>> _requests;

    QservStatus _qservStatus;  ///< Result to be returned
    size_t _numStarted = 0;    ///< The number of started requests
    size_t _numFinished = 0;   ///< The number of finished requests
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVSTATUSJOB_H
