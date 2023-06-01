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
#ifndef LSST_QSERV_WBASE_TASKSTATE_H
#define LSST_QSERV_WBASE_TASKSTATE_H

// System headers
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "global/intTypes.h"

namespace lsst::qserv::wbase {

/**
 * The class State represents transient states of the worker-side tasks.
 * @note This class and the relevant functions are put into this header to
 * allow the complile-time (only) dependency onto this type from other modules
 * without needing to link against the current module's library.
 * Also note a choice of the underlying type which is meant to allow sending
 * values of the type as numeric attribites in the Protobuf messages w/o
 * introducing an additional (Protobuf) representation for those, or converting
 * the values to strings and vs.
 */
enum class TaskState : std::uint64_t { CREATED = 0, QUEUED, EXECUTING_QUERY, READING_DATA, FINISHED };

/// @return The string representation of the input state.
/// @throw std::invalid_argument If the string can't be parsed into a valid state.
inline std::string taskState2str(TaskState state) {
    switch (state) {
        case TaskState::CREATED:
            return "CREATED";
        case TaskState::QUEUED:
            return "QUEUED";
        case TaskState::EXECUTING_QUERY:
            return "EXECUTING_QUERY";
        case TaskState::READING_DATA:
            return "READING_DATA";
        case TaskState::FINISHED:
            return "FINISHED";
        default:
            throw std::invalid_argument("wbase::" + std::string(__func__) + ": unsupported state " +
                                        std::to_string(static_cast<std::uint64_t>(state)));
    }
}

/// @return The parsed state of the input string.
/// @throw std::invalid_argument If the string can't be parsed into a valid state.
inline TaskState str2taskState(std::string const& state) {
    if (state == "CREATED")
        return TaskState::CREATED;
    else if (state == "QUEUED")
        return TaskState::QUEUED;
    else if (state == "EXECUTING_QUERY")
        return TaskState::EXECUTING_QUERY;
    else if (state == "READING_DATA")
        return TaskState::READING_DATA;
    else if (state == "FINISHED")
        return TaskState::FINISHED;
    throw std::invalid_argument("wbase::" + std::string(__func__) + ": unsupported state '" + state + "'");
}

/**
 * The structure TaskSelector is used in contexts where task filtering based on
 * values of parameters stored in the selector is required.
 */
struct TaskSelector {
    bool includeTasks = false;
    std::vector<QueryId> queryIds;
    std::vector<TaskState> taskStates;
    std::uint32_t maxTasks = 0U;
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_TASKSTATE_H
