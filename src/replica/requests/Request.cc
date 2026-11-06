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
#include "replica/requests/Request.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/requests/Messenger.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Request");

}  // namespace

namespace lsst::qserv::replica {

Request::Request(shared_ptr<Controller> const& controller, string const& type, string const& workerName,
                 int priority, bool keepTracking, bool disposeRequired)
        : RequestI(controller, type, workerName, priority, keepTracking, disposeRequired),
          _bufferPtr(new ProtocolBuffer(controller->serviceProvider()->config()->get<size_t>(
                  "common", "request-buf-size-bytes"))) {}

void Request::dispose(replica::Lock const& lock, int priority, OnDisposeCallbackType const& onFinish) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_DISPOSE);
    hdr.set_instance_id(controller()->serviceProvider()->config()->get<string>("security", "instance-id"));

    buffer()->serialize(hdr);
    ProtocolRequestDispose message;
    message.add_ids(id());
    buffer()->serialize(message);

    controller()->messenger()->send<ProtocolResponseDispose>(
            workerName(), id(), priority, buffer(),
            [onFinish](string const& id, bool success, ProtocolResponseDispose const& response) {
                if (onFinish) onFinish(success);
            });
}

}  // namespace lsst::qserv::replica
