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
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"


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
                                           bool saveReplicaInfo,
                                           CallbackType onFinish,
                                           int priority,
                                           bool keepTracking,
                                           std::shared_ptr<Messenger> const& messenger) {
    return FindAllRequest::Ptr(
        new FindAllRequest(serviceProvider,
                           io_service,
                           worker,
                           database,
                           saveReplicaInfo,
                           onFinish,
                           priority,
                           keepTracking,
                           messenger));
}

FindAllRequest::FindAllRequest(ServiceProvider::Ptr const& serviceProvider,
                               boost::asio::io_service& io_service,
                               std::string const& worker,
                               std::string const& database,
                               bool saveReplicaInfo,
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
        _saveReplicaInfo(saveReplicaInfo),
        _onFinish(onFinish) {

    Request::serviceProvider()->assertDatabaseIsValid(database);
}

const ReplicaInfoCollection& FindAllRequest::responseData() const {
    return _replicaInfoCollection;
}

void FindAllRequest::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    buffer()->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    buffer()->serialize(hdr);

    proto::ReplicationRequestFindAll message;
    message.set_priority(priority());
    message.set_database(database());

    buffer()->serialize(message);

    send(lock);
}

void FindAllRequest::wait(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.

    timer().expires_from_now(boost::posix_time::seconds(timerIvalSec()));
    timer().async_wait(
        boost::bind(
            &FindAllRequest::awaken,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void FindAllRequest::awaken(boost::system::error_code const& ec) {

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
    message.set_id(id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    buffer()->serialize(message);

    // Send the message

    send(lock);
}

void FindAllRequest::send(util::Lock const& lock) {

    auto self = shared_from_base<FindAllRequest>();

    messenger()->send<proto::ReplicationResponseFindAll>(
        worker(),
        id(),
        buffer(),
        [self] (std::string const& id,
                bool success,
                proto::ReplicationResponseFindAll const& response) {

            self->analyze(success,
                          response);
        }
    );
}

void FindAllRequest::analyze(bool success,
                             proto::ReplicationResponseFindAll const& message) {

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

    // Always use the latest status reported by the remote server

    setExtendedServerStatus(lock, replica::translate(message.status_ext()));

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (message.has_target_performance()) {
        mutablePerformance().update(message.target_performance());
    } else {
        mutablePerformance().update(message.performance());
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

            if (saveReplicaInfo()) {
                serviceProvider()->databaseServices()->saveReplicaInfoCollection(
                                                            worker(),
                                                            database(),
                                                            _replicaInfoCollection);
            }
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
                    "FindAllRequest::analyze() unknown status '" +
                    proto::ReplicationStatus_Name(message.status()) + "' received from server");
    }


}

void FindAllRequest::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<FindAllRequest>());
    }
}

void FindAllRequest::savePersistentState() {
    controller()->serviceProvider()->databaseServices()->saveState(*this);
}

std::string FindAllRequest::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              database());
}

}}} // namespace lsst::qserv::replica
