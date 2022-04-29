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

// Class header
#include "replica/DeleteWorkerTask.h"

// System headers
#include <atomic>
#include <vector>

// Qserv headers
#include "replica/DatabaseServices.h"

using namespace std;

namespace lsst { namespace qserv { namespace replica {

DeleteWorkerTask::Ptr DeleteWorkerTask::create(Controller::Ptr const& controller,
                                               Task::AbnormalTerminationCallbackType const& onTerminated,
                                               string const& worker, bool permanentDelete) {
    return Ptr(new DeleteWorkerTask(controller, onTerminated, worker, permanentDelete));
}

DeleteWorkerTask::DeleteWorkerTask(Controller::Ptr const& controller,
                                   Task::AbnormalTerminationCallbackType const& onTerminated,
                                   string const& worker, bool permanentDelete)
        : Task(controller, "EVICT-WORKER  ", onTerminated, 0),
          _worker(worker),
          _permanentDelete(permanentDelete) {}

void DeleteWorkerTask::onStart() {
    info(DeleteWorkerJob::typeName());

    string const noParentJobId;
    atomic<size_t> numFinishedJobs{0};
    vector<DeleteWorkerJob::Ptr> jobs;
    jobs.emplace_back(DeleteWorkerJob::create(
            _worker, _permanentDelete, controller(), noParentJobId,
            [&numFinishedJobs](DeleteWorkerJob::Ptr const& job) { ++numFinishedJobs; },
            serviceProvider()->config()->get<int>("controller", "worker-evict-priority-level")));
    jobs[0]->start();

    _logStartedEvent(jobs[0]);
    track<DeleteWorkerJob>(DeleteWorkerJob::typeName(), jobs, numFinishedJobs);
    _logStartedEvent(jobs[0]);
}

void DeleteWorkerTask::_logStartedEvent(DeleteWorkerJob::Ptr const& job) const {
    ControllerEvent event;

    event.operation = job->typeName();
    event.status = "STARTED";
    event.jobId = job->id();

    event.kvInfo.emplace_back("worker", _worker);

    logEvent(event);
}

void DeleteWorkerTask::_logFinishedEvent(DeleteWorkerJob::Ptr const& job) const {
    ControllerEvent event;

    event.operation = job->typeName();
    event.status = job->state2string();
    event.jobId = job->id();

    event.kvInfo = job->persistentLogData();
    event.kvInfo.emplace_back("worker", _worker);

    logEvent(event);
}

}}}  // namespace lsst::qserv::replica
