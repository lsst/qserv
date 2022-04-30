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
#include "replica/Task.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/QservSyncJob.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

using namespace std;

namespace lsst::qserv::replica {

TaskError::TaskError(util::Issue::Context const& ctx, string const& message)
        : util::Issue(ctx, "Task: " + message) {}

bool Task::start() {
    debug("starting...");

    util::Lock lock(_mtx, context() + __func__);

    if (_isRunning.exchange(true)) return true;

    thread t(bind(&Task::_startImpl, shared_from_this()));
    t.detach();

    return false;
}

bool Task::stop() {
    debug("stopping...");

    util::Lock lock(_mtx, context() + __func__);

    if (not _isRunning) return true;

    _stopRequested = true;

    return false;
}

bool Task::startAndWait(WaitEvaluatorType const& abortWait) {
    bool const wasRunning = start();

    auto self = shared_from_this();

    util::BlockPost blockPost(1000, 1001);  // ~1s
    while (isRunning()) {
        if ((nullptr != abortWait) and abortWait(self)) break;
        blockPost.wait();
    }

    return wasRunning;
}

Task::Task(Controller::Ptr const& controller, string const& name,
           Task::AbnormalTerminationCallbackType const& onTerminated, unsigned int const waitIntervalSec)
        : EventLogger(controller, name),
          _onTerminated(onTerminated),
          _waitIntervalSec(waitIntervalSec),
          _isRunning(false),
          _stopRequested(false),
          _numFinishedJobs(0),
          _log(LOG_GET("lsst.qserv.replica.Task")) {
    debug("created");
}

string Task::context() const { return _name + " "; }

void Task::sync(unsigned int qservSyncTimeoutSec, bool forceQservSync) {
    launch<QservSyncJob>(PRIORITY_NORMAL, qservSyncTimeoutSec, forceQservSync);
}

void Task::_startImpl() {
    // By design of this class, any but TaskStopped exceptions thrown
    // by a subclass-specific method called below will be interpreted as
    // the "abnormal" termination condition to be reported via an optional
    // callback.

    bool terminated = false;
    try {
        debug("started");
        logOnStartEvent();
        onStart();

        util::BlockPost blockPost(1000 * _waitIntervalSec, 1000 * _waitIntervalSec + 1);

        while (not stopRequested() and onRun()) {
            blockPost.wait();
        }
        debug("stopped");
        logOnStopEvent();
        onStop();

    } catch (TaskStopped const&) {
        debug("stopped");
        logOnStopEvent();
        onStop();

    } catch (exception const& ex) {
        string const msg = ex.what();
        error("terminated, exception: " + msg);
        logOnTerminatedEvent(msg);
        terminated = true;
    }

    // This lock is needed to ensure thread safety when making changes
    // to the object's state.
    //
    // Note that the object state needs to be updated before making
    // the emergency upstream notification. The notification is made
    // via a non-blocking mechanism by scheduling the callback to be
    // run in a different thread with the life expectancy of the current
    // object guaranteed through a copy of a shared pointer bound as
    // a parameter of the callback.

    util::Lock lock(_mtx, context() + __func__);

    _stopRequested = false;
    _isRunning = false;

    if (terminated) {
        serviceProvider()->io_service().post(bind(_onTerminated, shared_from_this()));
    }
}

}  // namespace lsst::qserv::replica
