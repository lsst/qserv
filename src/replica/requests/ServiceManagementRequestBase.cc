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
#include "replica/requests/ServiceManagementRequestBase.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/requests/Messenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ServiceManagementRequest");

bool const keepTrackingNo = false;
bool const disposeRequiredNo = false;

}  // namespace

namespace lsst::qserv::replica {

void ServiceManagementRequestBase::extendedPrinter(shared_ptr<ServiceManagementRequestBase> const& ptr) {
    Request::defaultPrinter(ptr);
    cout << ptr->getServiceState();
}

ServiceStateInfo const& ServiceManagementRequestBase::getServiceState() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    switch (Request::state()) {
        case Request::State::FINISHED:
            switch (Request::extendedState()) {
                case Request::ExtendedState::SUCCESS:
                case Request::ExtendedState::SERVER_ERROR:
                    return _serviceState;
                default:
                    break;
            }
        default:
            break;
    }
    throw logic_error("ServiceManagementRequestBase::" + string(__func__) +
                      "  not allowed in the current state of the request");
}

ServiceManagementRequestBase::ServiceManagementRequestBase(shared_ptr<Controller> const& controller,
                                                           char const* requestName, string const& workerName,
                                                           ProtocolServiceRequestType requestType,
                                                           int priority)
        : Request(controller, requestName, workerName, priority, ::keepTrackingNo, ::disposeRequiredNo),
          _requestType(requestType) {}

void ServiceManagementRequestBase::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Serialize the Request message header and the request itself into
    // the network buffer.
    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::SERVICE);
    hdr.set_service_type(_requestType);
    hdr.set_instance_id(controller()->serviceProvider()->config()->get<string>("security", "instance-id"));
    buffer()->serialize(hdr);

    // Send the message
    controller()->messenger()->send<ProtocolServiceResponse>(
            workerName(), id(), priority(), buffer(),
            [self = shared_from_base<ServiceManagementRequestBase>()](
                    string const& id, bool success, ProtocolServiceResponse const& response) {
                self->_analyze(success, response);
            });
}

void ServiceManagementRequestBase::_analyze(bool success, ProtocolServiceResponse const& message) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method send() - the only
    // client of _analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;
    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    // Update performance counters.
    mutablePerformance().update(message.performance());

    // Capture the general status of the operation.
    switch (protocol::toStatus(message.status())) {
        case protocol::Status::SUCCESS:
            // Transfer the state of the remote service into a local data member
            // before initiating state transition of the request object.
            _serviceState.set(message);
            finish(lock, SUCCESS);
            break;
        default:
            finish(lock, SERVER_ERROR);
            break;
    }
}

void ServiceManagementRequestBase::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

}  // namespace lsst::qserv::replica
