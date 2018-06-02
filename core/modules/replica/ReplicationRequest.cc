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
#include "replica/ReplicationRequest.h"

// System headers
#include <future>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

ReplicationRequest::Ptr ReplicationRequest::create(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    CallbackType onFinish,
                                    int  priority,
                                    bool keepTracking,
                                    bool allowDuplicate,
                                    std::shared_ptr<Messenger> const& messenger) {

    return ReplicationRequest::Ptr(
        new ReplicationRequest(
            serviceProvider,
            io_service,
            worker,
            sourceWorker,
            database,
            chunk,
            onFinish,
            priority,
            keepTracking,
            allowDuplicate,
            messenger));
}

ReplicationRequest::ReplicationRequest(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    CallbackType onFinish,
                                    int  priority,
                                    bool keepTracking,
                                    bool allowDuplicate,
                                    std::shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         "REPLICA_CREATE",
                         worker,
                         priority,
                         keepTracking,
                         allowDuplicate,
                         messenger),
        _database(database),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _onFinish(onFinish),
        _replicaInfo() {

    Request::serviceProvider()->assertWorkerIsValid(sourceWorker);
    Request::serviceProvider()->assertWorkersAreDifferent(sourceWorker, worker);
    Request::serviceProvider()->assertDatabaseIsValid(database);
}

void ReplicationRequest::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    buffer()->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    buffer()->serialize(hdr);

    proto::ReplicationRequestReplicate message;
    message.set_priority(priority());
    message.set_database(database());
    message.set_chunk(chunk());
    message.set_worker(sourceWorker());

    buffer()->serialize(message);

    send(lock);
}

void ReplicationRequest::wait(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.

    timer().expires_from_now(boost::posix_time::seconds(timerIvalSec()));
    timer().async_wait(
        boost::bind(
            &ReplicationRequest::awaken,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void ReplicationRequest::awaken(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "awaken");

    if (state() == State::FINISHED) return;

    // Serialize the Status message header and the request itself into
    // the network buffer.

    buffer()->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    buffer()->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(remoteId());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    buffer()->serialize(message);

    send(lock);
}

void ReplicationRequest::send(util::Lock const& lock) {

    auto self = shared_from_base<ReplicationRequest>();

    messenger()->send<proto::ReplicationResponseReplicate>(
        worker(),
        id(),
        buffer(),
        [self] (std::string const& id,
                bool success,
                proto::ReplicationResponseReplicate const& response) {

            self->analyze(success,
                          response);
        }
    );
}

void ReplicationRequest::analyze(bool success,
                                 proto::ReplicationResponseReplicate const& message) {

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

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "analyze");

    if (state() == State::FINISHED) return;

    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    // Allways get the latest status reported by the remote server

    setExtendedServerStatus(lock, replica::translate(message.status_ext()));

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (message.has_target_performance()) mutablePerformance().update(message.target_performance());
    else                                  mutablePerformance().update(message.performance());

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    _replicaInfo = ReplicaInfo(&(message.replica_info()));

    // Extract target request type-specific parameters from the response
    if (message.has_request()) {
        _targetRequestParams = ReplicationRequestParams(message.request());
    }
    switch (message.status()) {

        case proto::ReplicationStatus::SUCCESS:

            serviceProvider()->databaseServices()->saveReplicaInfo(_replicaInfo);

            finish(lock, SUCCESS);
            break;

        case proto::ReplicationStatus::QUEUED:
            if (keepTracking()) wait(lock);
            else                finish(lock, SERVER_QUEUED);
            break;

        case proto::ReplicationStatus::IN_PROGRESS:
            if (keepTracking()) wait(lock);
            else                finish(lock, SERVER_IN_PROGRESS);
            break;

        case proto::ReplicationStatus::IS_CANCELLING:
            if (keepTracking()) wait(lock);
            else                finish(lock, SERVER_IS_CANCELLING);
            break;

        case proto::ReplicationStatus::BAD:

            // Special treatment of the duplicate requests if allowed

            if (extendedServerStatus() == ExtendedCompletionStatus::EXT_STATUS_DUPLICATE) {

                setDuplicateRequestId(lock, message.duplicate_request_id());

                if (allowDuplicate() && keepTracking()) {
                    wait(lock);
                    return;
                }
            }
            finish(lock, SERVER_BAD);
            break;

        case proto::ReplicationStatus::FAILED:
            finish(lock, SERVER_ERROR);
            break;

        case proto::ReplicationStatus::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;

        default:
            throw std::logic_error(
                    "ReplicationRequest::analyze() unknown status '" +
                    proto::ReplicationStatus_Name(message.status()) +
                    "' received from server");
    }
}

void ReplicationRequest::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<ReplicationRequest>());
    }
}

void ReplicationRequest::savePersistentState(util::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

std::string ReplicationRequest::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              database(),
                              chunk(),
                              sourceWorker());
}

}}} // namespace lsst::qserv::replica
