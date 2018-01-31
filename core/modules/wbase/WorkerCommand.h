// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
/// WorkerCommand.h
#ifndef LSST_QSERV_WBASE_WORKER_COMMAND_H
#define LSST_QSERV_WBASE_WORKER_COMMAND_H

// System headers
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "util/Command.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
class SendChannel;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wbase {

/**
  * Class WorkerCommand is the base class for a family of various worker
  * management commmands.
  */
class WorkerCommand
    :   public util::Command {

public:

    /// The smart pointer type to objects of the class
    using Ptr =  std::shared_ptr<WorkerCommand>;

    // The default construction and copy semantics are prohibited
    WorkerCommand& operator=(const WorkerCommand&) = delete;
    WorkerCommand(const WorkerCommand&) = delete;
    WorkerCommand() = delete;

    /**
     * The normal constructor of the class
     * @param sendChannel - communication channel for reporting results
     */
    explicit WorkerCommand(std::shared_ptr<SendChannel> const& sendChannel);

    /// The destructor
    virtual ~WorkerCommand();

    /**
     * The code which will be execute by specific subclasses of this abstract class
     */
    virtual void run ()=0;

protected:

    std::shared_ptr<SendChannel> _sendChannel;  ///< For result reporting
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_WORKER_COMMAND_H
