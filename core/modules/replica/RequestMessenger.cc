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
#include "replica/RequestMessenger.h"

// Qserv headers
#include "replica/protocol.pb.h"
#include "replica/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RequestMessenger");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

RequestMessenger::RequestMessenger(ServiceProvider::Ptr const& serviceProvider,
                                   boost::asio::io_service& io_service,
                                   string const& type,
                                   string const& worker,
                                   int  priority,
                                   bool keepTracking,
                                   bool allowDuplicate,
                                   bool disposeRequired,
                                   Messenger::Ptr const& messenger)
    :   Request(serviceProvider,
                io_service,
                type,
                worker,
                priority,
                keepTracking,
                allowDuplicate,
                disposeRequired),
        _messenger(messenger) {
}


void RequestMessenger::finishImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Make sure the request (if any) has been eliminated from the messenger
    if (_messenger->exists(worker(), id())) {
        _messenger->cancel(worker(), id());
    }

    // Tell the worker to dispose the request should this be requested
    if (disposeRequired()) {

        buffer()->resize();

        ProtocolRequestHeader hdr;
        hdr.set_id(id());
        hdr.set_type(ProtocolRequestHeader::REQUEST);
        hdr.set_management_type(ProtocolManagementRequestType::REQUEST_DISPOSE);
        hdr.set_instance_id(serviceProvider()->instanceId());

        buffer()->serialize(hdr);
        ProtocolRequestDispose message;
        message.add_ids(id());
        buffer()->serialize(message);

        _messenger->send<ProtocolResponseDispose>(
            worker(),
            id(),
            buffer(),
            // Don't require any callback notification for the completion of
            // the operation. This will also prevent incrementing a shared pointer
            // counter for the current object.
            nullptr
        );
    }
}

}}} // namespace lsst::qserv::replica
