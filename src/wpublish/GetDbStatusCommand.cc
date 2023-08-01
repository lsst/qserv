// -*- LSST-C++ -*-
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

// Class header
#include "wpublish/GetDbStatusCommand.h"

// System headers
#include <stdexcept>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "mysql/MySqlUtils.h"
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"
#include "wconfig/WorkerConfig.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.GetDbStatusCommand");

}  // anonymous namespace

namespace lsst::qserv::wpublish {

GetDbStatusCommand::GetDbStatusCommand(shared_ptr<wbase::SendChannel> const& sendChannel)
        : wbase::WorkerCommand(sendChannel) {}

void GetDbStatusCommand::run() {
    string const context = "GetDbStatusCommand::" + string(__func__);
    LOGS(_log, LOG_LVL_DEBUG, context);

    json result;
    try {
        bool const full = true;
        result = mysql::MySqlUtils::processList(wconfig::WorkerConfig::instance()->getMySqlConfig(), full);
    } catch (mysql::MySqlQueryError const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << " " << ex.what());
        reportError<proto::WorkerCommandGetDbStatusR>(ex.what());
        return;
    }
    proto::WorkerCommandGetDbStatusR reply;
    reply.mutable_status();
    reply.set_info(result.dump());

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);

    LOGS(_log, LOG_LVL_DEBUG, context << "  ** SENT **");
}

}  // namespace lsst::qserv::wpublish
