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
#include "wbase/WorkerCommand.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wbase/SendChannel.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.WorkerCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wbase {

WorkerCommand::WorkerCommand(SendChannel::Ptr const& sendChannel)
    :   _sendChannel(sendChannel) {
    
    // Register a function which will run a subclass-specific
    // implementation of method run()
    setFunc([this] (util::CmdData* data) {
        this->run();
    });
}

WorkerCommand::~WorkerCommand() {
}

}}} // namespace lsst::qserv::wbase
