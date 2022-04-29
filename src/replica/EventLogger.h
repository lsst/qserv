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
#ifndef LSST_QSERV_REPLICA_EVENTLOGGER_H
#define LSST_QSERV_REPLICA_EVENTLOGGER_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/Controller.h"
#include "replica/Job.h"

// Forward declarations
namespace lsst { namespace qserv { namespace replica {
struct ControllerEvent;
}}}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class EventLogger is the base class for logging Controller events on behalf
 * of tasks or other activities.
 */
class EventLogger {
public:
    // Default construction and copy semantics is prohibited

    EventLogger() = delete;
    EventLogger(EventLogger const&) = delete;
    EventLogger& operator=(EventLogger const&) = delete;

    virtual ~EventLogger() = default;

    /// @return a reference to the Controller
    Controller::Ptr const& controller() const { return _controller; }

    /// @return the name of a task/activity
    std::string const& name() const { return _name; }

protected:
    /**
     * The constructor is available to subclasses only
     *
     * @param controller  reference to the Controller for accessing services.
     * @param name  the name of a task/activity (used for logging info into
     *              the log stream, and for logging events into the persistent log)
     */
    EventLogger(Controller::Ptr const& controller, std::string const& name);

    /// Log an event in the persistent log
    void logEvent(ControllerEvent& event) const;

    /// Log the very first event to report the start of a task/activity
    void logOnStartEvent() const;

    /// Log an event to report the end of a task/activity
    void logOnStopEvent() const;

    /**
     * Log an event to report the termination of the task.
     *
     * @param msg  error message to be reported
     */
    void logOnTerminatedEvent(std::string const& msg) const;

    /**
     * Reported the start of a job
     *
     * @param typeName  the type name of the job
     * @param job       pointer to the job
     * @param family    the name of a database family
     */
    void logJobStartedEvent(std::string const& typeName, Job::Ptr const& job,
                            std::string const& family) const;

    /**
     * Reported the finish of a job
     *
     * @param typeName  the type name of the job
     * @param job       pointer to the job
     * @param family    the name of a database family
     */
    void logJobFinishedEvent(std::string const& typeName, Job::Ptr const& job,
                             std::string const& family) const;

    // Input parameters

    Controller::Ptr const _controller;
    std::string const _name;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_EVENTLOGGER_H
