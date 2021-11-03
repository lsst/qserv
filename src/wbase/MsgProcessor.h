
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

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {

class Task;
class WorkerCommand;

}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wbase {

/// MsgProcessor implementations handle incoming Task objects.
struct MsgProcessor {

    virtual ~MsgProcessor() {}

    /// Process a query processing task
    virtual void processTask(std::shared_ptr<wbase::Task> const& task) = 0;

    /// Process a managememt command
    virtual void processCommand(std::shared_ptr<wbase::WorkerCommand> const& command) = 0;

    /// @return a JSON representation of the object's status for the monitoring
    virtual nlohmann::json statusToJson() = 0;
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_MSG_PROCESSOR_H