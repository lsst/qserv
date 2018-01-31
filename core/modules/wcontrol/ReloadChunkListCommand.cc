// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2018 AURA/LSST.
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
#include "wcontrol/ReloadChunkListCommand.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"

// Qserv headers

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.ReloadChunkListCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wcontrol {

ReloadChunkListCommand::ReloadChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel)
    :   wbase::WorkerCommand(sendChannel) {
}

ReloadChunkListCommand::~ReloadChunkListCommand() {
}

void
ReloadChunkListCommand::run() {

    std::string const msg = "ReloadChunkListCommand::run  ** NOT IMPLEMENTED **";

    LOGS(_log, LOG_LVL_DEBUG, msg);

    // Send back a protobuf object with the status of the operation
    proto::WorkerCmdReply reply;
    reply.set_status(proto::WorkerCmdReply::SUCCESS);

    std::string replyString;
    reply.SerializeToString(&replyString);

    _sendChannel->send(replyString.data(), replyString.size());
}

}}} // namespace lsst::qserv::wcontrol
