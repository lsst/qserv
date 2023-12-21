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
#include "replica/contr/EventLogger.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.EventLogger");
}

namespace lsst::qserv::replica {

EventLogger::EventLogger(Controller::Ptr const& controller, string const& name)
        : _controller(controller), _name(name) {}

void EventLogger::logEvent(ControllerEvent& event) const {
    // Finish filling the common fields

    event.controllerId = controller()->identity().id;
    event.timeStamp = util::TimeUtils::now();
    event.task = name();

    // For now ignore exceptions when logging events. Just report errors.
    try {
        controller()->serviceProvider()->databaseServices()->logControllerEvent(event);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             name() << "  "
                    << "failed to log event in " << __func__);
    }
}

void EventLogger::logOnStartEvent() const {
    ControllerEvent event;
    event.status = "STARTED";
    logEvent(event);
}

void EventLogger::logOnStopEvent() const {
    ControllerEvent event;

    event.status = "STOPPED";

    logEvent(event);
}

void EventLogger::logOnTerminatedEvent(string const& msg) const {
    ControllerEvent event;

    event.status = "TERMINATED";
    event.kvInfo.emplace_back("error", msg);

    logEvent(event);
}

void EventLogger::logJobStartedEvent(string const& typeName, Job::Ptr const& job,
                                     string const& family) const {
    ControllerEvent event;

    event.operation = typeName;
    event.status = "STARTED";
    event.jobId = job->id();

    event.kvInfo.emplace_back("database-family", family);

    logEvent(event);
}

void EventLogger::logJobFinishedEvent(string const& typeName, Job::Ptr const& job,
                                      string const& family) const {
    ControllerEvent event;

    event.operation = typeName;
    event.status = job->state2string();
    event.jobId = job->id();

    event.kvInfo = job->persistentLogData();
    event.kvInfo.emplace_back("database-family", family);

    logEvent(event);
}

}  // namespace lsst::qserv::replica
