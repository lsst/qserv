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
#ifndef LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H
#define LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H

// System headers
#include <string>

// Qserv headers
#include "wbase/TaskState.h"
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst::qserv {
namespace wbase {
class MsgProcessor;
class SendChannel;
}  // namespace wbase
namespace wpublish {
class ResourceMonitor;
}
}  // namespace lsst::qserv

// This header declarations
namespace lsst::qserv::wpublish {

/**
 * Class GetStatusCommand returns various info on the on-going status of
 * a Qserv worker.
 */
class GetStatusCommand : public wbase::WorkerCommand {
public:
    /**
     * @param sendChannel      The communication channel for reporting results.
     * @param processor        The message processor for extracting status info.
     * @param resourceMonitor  The XRootD resource monitor for finding which chunks are in use.
     * @param taskSelector     Task selection criterias.
     */
    GetStatusCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                     std::shared_ptr<wbase::MsgProcessor> const& processor,
                     std::shared_ptr<ResourceMonitor> const& resourceMonitor,
                     wbase::TaskSelector const& taskSelector);

    GetStatusCommand() = delete;
    GetStatusCommand& operator=(GetStatusCommand const&) = delete;
    GetStatusCommand(GetStatusCommand const&) = delete;

    virtual ~GetStatusCommand() override = default;

    virtual void run() override;

private:
    // Parameters of the object

    std::shared_ptr<wbase::MsgProcessor> const _processor;
    std::shared_ptr<ResourceMonitor> const _resourceMonitor;
    wbase::TaskSelector const _taskSelector;
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H
