
// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2016 LSST Corporation.
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
/// MsgProcessor.h
#ifndef LSST_QSERV_WBASE_MSG_PROCESSOR_H
#define LSST_QSERV_WBASE_MSG_PROCESSOR_H

// System headers
#include <memory>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst::qserv::wbase {
class Task;
struct TaskSelector;
class WorkerCommand;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wbase {

/// MsgProcessor implementations handle incoming Task objects.
struct MsgProcessor { // &&& delete file if possible
    virtual ~MsgProcessor() {}

    /// Process a group of query processing tasks.
    virtual void processTasks(std::vector<std::shared_ptr<wbase::Task>> const& tasks) = 0; // &&& delete

    /// Process a managememt command
    virtual void processCommand(std::shared_ptr<wbase::WorkerCommand> const& command) = 0; // &&& can this be deleted

    /**
     * Retreive the status of queries being processed by the worker.
     * @param taskSelector Task selection criterias.
     * @return a JSON representation of the object's status for the monitoring
     */
    virtual nlohmann::json statusToJson(wbase::TaskSelector const& taskSelector) = 0; // &&& can this be deleted
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_MSG_PROCESSOR_H
