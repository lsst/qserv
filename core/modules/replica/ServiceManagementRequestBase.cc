/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/ServiceManagementRequestBase.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/Messenger.h"
#include "replica/LockUtils.h"
#include "replica/Performance.h"
#include "replica/ProtocolBuffer.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ServiceManagementRequest");

namespace proto = lsst::qserv::proto;

/// Dump a collection of request descriptions onto the output stream
void dumpRequestInfo(std::ostream& os,
                     std::vector<proto::ReplicationServiceResponseInfo> const& requests) {

    for (const auto &r : requests) {
        os  << "\n"
            << "    type:     " << proto::ReplicationReplicaRequestType_Name(r.replica_type()) << "\n"
            << "    id:       " << r.id() << "\n"
            << "    priority: " << r.priority() << "\n"
            << "    database: " << r.database() << "\n";

        switch (r.replica_type()) {
            case proto::ReplicationReplicaRequestType::REPLICA_CREATE:
                os  << "    chunk:    " << r.chunk() << "\n"
                    << "    worker:   " << r.worker() << "\n";
                break;

            case proto::ReplicationReplicaRequestType::REPLICA_DELETE:
            case proto::ReplicationReplicaRequestType::REPLICA_FIND:
                os  << "    chunk:    " << r.chunk() << "\n";
                break;

            case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL:
                break;

            default:
                throw std::logic_error (
                            "unhandled request type " +
                            proto::ReplicationReplicaRequestType_Name(r.replica_type()) +
                            " in  ServiceManagementRequest::dumpRequestInfo");
        }
    }
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////
//         ServiceState          //
///////////////////////////////////

void ServiceState::set(proto::ReplicationServiceResponse const& message) {

    switch (message.service_state()) {

        case proto::ReplicationServiceResponse::SUSPEND_IN_PROGRESS:
            state = ServiceState::State::SUSPEND_IN_PROGRESS;
            break;

        case proto::ReplicationServiceResponse::SUSPENDED:
            state = ServiceState::State::SUSPENDED;
            break;

        case proto::ReplicationServiceResponse::RUNNING:
            state = ServiceState::State::RUNNING;
            break;

        default:
            throw std::runtime_error(
                "ServiceState::set() service state found in protocol is unknown");
    }
    technology = message.technology();
    startTime  = message.start_time();

    numNewRequests        = message.num_new_requests();
    numInProgressRequests = message.num_in_progress_requests();
    numFinishedRequests   = message.num_finished_requests();

    for (int num = message.new_requests_size(), idx = 0; idx < num; ++idx) {
        newRequests.emplace_back(message.new_requests(idx));
    }
    for (int num = message.in_progress_requests_size(), idx = 0; idx < num; ++idx) {
        inProgressRequests.emplace_back(message.in_progress_requests(idx));
    }
    for (int num = message.finished_requests_size(), idx = 0; idx < num; ++idx) {
       finishedRequests.emplace_back(message.finished_requests(idx));
    }
}

std::ostream& operator<<(std::ostream& os, ServiceState const& ss) {

    unsigned int const secondsAgo = (PerformanceUtils::now() - ss.startTime) / 1000.0f;

    os  << "ServiceState:\n"
        << "\n  Summary:\n\n"
        << "    service state:              " << ss.state2string() << "\n"
        << "    technology:                 " << ss.technology << "\n"
        << "    start time [ms]:            " << ss.startTime << " (" << secondsAgo << " seconds ago)\n"
        << "    total new requests:         " << ss.numNewRequests << "\n"
        << "    total in-progress requests: " << ss.numInProgressRequests << "\n"
        << "    total finished requests:    " << ss.numFinishedRequests << "\n";

    os  << "\n  New:\n";
    ::dumpRequestInfo(os, ss.newRequests);

    os  << "\n  In-Progress:\n";
    ::dumpRequestInfo(os, ss.inProgressRequests);

    os  << "\n  Finished:\n";
    ::dumpRequestInfo(os, ss.finishedRequests);

    return os;
}

ServiceState const& ServiceManagementRequestBase::getServiceState() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getServiceState");

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
    throw std::logic_error(
                    "this informationis not available in the current state of the request");
}

ServiceManagementRequestBase::ServiceManagementRequestBase(
                                    ServiceProvider::Ptr const&      serviceProvider,
                                    boost::asio::io_service&             io_service,
                                    char const*                          requestTypeName,
                                    std::string const&                   worker,
                                    proto::ReplicationServiceRequestType requestType,
                                    std::shared_ptr<Messenger> const&    messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         requestTypeName,
                         worker,
                         0,        /* priority */
                         false,    /* keepTracking */
                         false,    /* allowDuplicate */
                         messenger),
        _requestType(requestType) {
}

void ServiceManagementRequestBase::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    ASSERT_LOCK(_mtx, context() + "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::SERVICE);
    hdr.set_service_type(_requestType);

    _bufferPtr->serialize(hdr);

    // Send the message

    auto self = shared_from_base<ServiceManagementRequestBase>();

    _messenger->send<proto::ReplicationServiceResponse>(
        worker(),
        id(),
        _bufferPtr,
        [self] (std::string const& id,
                bool success,
                proto::ReplicationServiceResponse const& response) {
            self->analyze(success, response);
        }
    );
}

void ServiceManagementRequestBase::analyze(bool success,
                                           proto::ReplicationServiceResponse const& message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronious callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occure while the async I/O was
    // still in a progress.

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "analyze");

    if (_state == State::FINISHED) return;

    if (success) {
        _performance.update(message.performance());

        // Capture the general status of the operation

        switch (message.status()) {

            case proto::ReplicationServiceResponse::SUCCESS:

                // Transfer the state of the remote service into a local data member
                // before initiating state transition of the request object.

                _serviceState.set(message);

                finish(SUCCESS);
                break;

            default:
                finish(SERVER_ERROR);
                break;
        }
    } else {
        finish(CLIENT_ERROR);
    }

    if (_state == State::FINISHED) notify();
}

}}} // namespace lsst::qserv::replica
