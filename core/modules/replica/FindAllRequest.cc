/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/FindAllRequest.h"

// System headers
#include <future>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<std::mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

FindAllRequest::Ptr FindAllRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                           boost::asio::io_service& io_service,
                                           std::string const& worker,
                                           std::string const& database,
                                           CallbackType onFinish,
                                           int priority,
                                           bool keepTracking,
                                           std::shared_ptr<Messenger> const& messenger) {
    return FindAllRequest::Ptr(
        new FindAllRequest(serviceProvider,
                           io_service,
                           worker,
                           database,
                           onFinish,
                           priority,
                           keepTracking,
                           messenger));
}

FindAllRequest::FindAllRequest(ServiceProvider::Ptr const& serviceProvider,
                               boost::asio::io_service& io_service,
                               std::string const& worker,
                               std::string const& database,
                               CallbackType onFinish,
                               int  priority,
                               bool keepTracking,
                               std::shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         "REPLICA_FIND_ALL",
                         worker,
                         priority,
                         keepTracking,
                         false, /* allowDuplicate */
                         messenger),
        _database(database),
        _onFinish(onFinish),
        _replicaInfoCollection() {

    _serviceProvider->assertDatabaseIsValid (database);
}

const ReplicaInfoCollection& FindAllRequest::responseData() const {
    return _replicaInfoCollection;
}

void FindAllRequest::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestFindAll message;
    message.set_priority(priority());
    message.set_database(database());

    _bufferPtr->serialize(message);

    send();
}

void FindAllRequest::wait() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.

    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
        boost::bind(
            &FindAllRequest::awaken,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void FindAllRequest::awaken(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    LOCK(_mtx);

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state == State::FINISHED) return;

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(message);

    // Send the message

    send();
}

void FindAllRequest::send() {

    auto self = shared_from_base<FindAllRequest>();

    _messenger->send<proto::ReplicationResponseFindAll>(
        worker(),
        id(),
        _bufferPtr,
        [self] (std::string const& id,
                bool success,
                proto::ReplicationResponseFindAll const& response) {
            self->analyze (success, response);
        }
    );
}

void FindAllRequest::analyze(bool success,
                             proto::ReplicationResponseFindAll const& message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  success=" << (success ? "true" : "false"));

    // This guard is made on behalf of an asynchronious callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze()
    LOCK(_mtx);

    if (success) {

        // Always get the latest status reported by the remote server
        _extendedServerStatus = replica::translate(message.status_ext());

        // Performance counters are updated from either of two sources,
        // depending on the availability of the 'target' performance counters
        // filled in by the 'STATUS' queries. If the later is not available
        // then fallback to the one of the current request.

        if (message.has_target_performance()) {
            _performance.update(message.target_performance());
        } else {
            _performance.update(message.performance());
        }

        // Always extract extended data regardless of the completion status
        // reported by the worker service.

        for (int num = message.replica_info_many_size(), idx = 0; idx < num; ++idx) {
            _replicaInfoCollection.emplace_back(&(message.replica_info_many(idx)));
        }

        // Extract target request type-specific parameters from the response
        if (message.has_request()) {
            _targetRequestParams = FindAllRequestParams(message.request());
        }
        switch (message.status()) {

            case proto::ReplicationStatus::SUCCESS:

                // Save the replica state
                _serviceProvider->databaseServices()->saveReplicaInfoCollection(worker(),
                                                                                database(),
                                                                                _replicaInfoCollection);

                finish(SUCCESS);
                break;

            case proto::ReplicationStatus::QUEUED:
                if (_keepTracking) wait();
                else               finish(SERVER_QUEUED);
                break;

            case proto::ReplicationStatus::IN_PROGRESS:
                if (_keepTracking) wait();
                else               finish(SERVER_IN_PROGRESS);
                break;

            case proto::ReplicationStatus::IS_CANCELLING:
                if (_keepTracking) wait();
                else               finish(SERVER_IS_CANCELLING);
                break;

            case proto::ReplicationStatus::BAD:
                finish(SERVER_BAD);
                break;

            case proto::ReplicationStatus::FAILED:
                finish(SERVER_ERROR);
                break;

            case proto::ReplicationStatus::CANCELLED:
                finish(SERVER_CANCELLED);
                break;

            default:
                throw std::logic_error(
                        "FindAllRequest::analyze() unknown status '" +
                        proto::ReplicationStatus_Name(message.status()) + "' received from server");
        }

    } else {
        finish(CLIENT_ERROR);
    }
    if (_state == State::FINISHED) notify();
}

void FindAllRequest::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish != nullptr) {
        auto self = shared_from_base<FindAllRequest>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

}}} // namespace lsst::qserv::replica
