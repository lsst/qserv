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
#include "wcontrol/TestEchoCommand.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"

// Qserv headers

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.TestEchoCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wcontrol {

TestEchoCommand::TestEchoCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                 std::string const& value)
    :   wbase::WorkerCommand(sendChannel),
        _value(value) {
}

TestEchoCommand::~TestEchoCommand() {
}

void
TestEchoCommand::run() {

    LOGS(_log, LOG_LVL_DEBUG, "TestEchoCommand::run");

    proto::WorkerCommandTestEchoR reply;
    reply.set_status(proto::WorkerCommandTestEchoR::SUCCESS);
    reply.set_value (_value);

    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

}}} // namespace lsst::qserv::wcontrol