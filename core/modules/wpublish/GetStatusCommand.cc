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
#include "wpublish/GetStatusCommand.h"

// Qserv headers
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.GetStatusCommand");

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

GetStatusCommand::GetStatusCommand(shared_ptr<wbase::SendChannel> const& sendChannel)
    :   wbase::WorkerCommand(sendChannel) {
}


void GetStatusCommand::run() {

    LOGS(_log, LOG_LVL_DEBUG, "GetStatusCommand::" << __func__);

    proto::WorkerCommandGetStatusR reply;
    reply.set_info("{}");

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);
}

}}} // namespace lsst::qserv::wpublish
