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
#include "replica/FindRequest.h"

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
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

FindRequest::Ptr FindRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                     boost::asio::io_service& io_service,
                                     std::string const& worker,
                                     std::string const& database,
                                     unsigned int chunk,
                                     CallbackType onFinish,
                                     int priority,
                                     bool computeCheckSum,
                                     bool keepTracking,
                                     std::shared_ptr<Messenger> const& messenger) {
    return FindRequest::Ptr(
        new FindRequest(serviceProvider,
                        io_service,
                        worker,
                        database,
                        chunk,
                        onFinish,
                        priority,
                        computeCheckSum,
                        keepTracking,
                        messenger));
}

FindRequest::FindRequest(ServiceProvider::Ptr const& serviceProvider,
                           boost::asio::io_service& io_service,
                           std::string const& worker,
                           std::string const& database,
                           unsigned int  chunk,
                           CallbackType onFinish,
                           int  priority,
                           bool computeCheckSum,
                           bool keepTracking,
                           std::shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         "REPLICA_FIND",
                         worker,
                         priority,
                         keepTracking,
                         false /* allowDuplicate */,
                         messenger),
        _database(database),
        _chunk(chunk),
        _computeCheckSum(computeCheckSum),
        _onFinish(onFinish),
        _replicaInfo() {

    _serviceProvider->assertDatabaseIsValid(database);
}

ReplicaInfo const& FindRequest::responseData() const {
    return _replicaInfo;
}


void FindRequest::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl "
         << " worker: "          << worker()
         << " database: "        << database()
         << " chunk: "           << chunk()
         << " computeCheckSum: " << (computeCheckSum() ? "true" : "false"));

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestFind message;
    message.set_priority(priority());
    message.set_database(database());
    message.set_chunk(chunk());
    message.set_compute_cs(computeCheckSum());

    _bufferPtr->serialize(message);

    send(lock);
}

void FindRequest::wait(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.

    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
        boost::bind(
            &FindRequest::awaken,
            shared_from_base<FindRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void FindRequest::awaken(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "awaken");

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
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND);

    _bufferPtr->serialize(message);

    send(lock);
}

void FindRequest::send(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "send");

    auto self = shared_from_base<FindRequest>();

    _messenger->send<proto::ReplicationResponseFind>(
        worker(),
        id(),
        _bufferPtr,
        [self] (std::string const& id,
                bool success,
                proto::ReplicationResponseFind const& response) {

            self->analyze(success,
                          response);
        }
    );
}

void FindRequest::analyze(bool success,
                          proto::ReplicationResponseFind const& message) {

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

    util::Lock lock(_mtx, context() + "analyze");

    if (_state == State::FINISHED) return;

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

        _replicaInfo = ReplicaInfo(&(message.replica_info()));

        // Extract target request type-specific parameters from the response
        if (message.has_request()) {
            _targetRequestParams = FindRequestParams(message.request());
        }
        switch (message.status()) {

            case proto::ReplicationStatus::SUCCESS:

                _serviceProvider->databaseServices()->saveReplicaInfo(_replicaInfo);

                finish(lock,
                       SUCCESS);
                break;

            case proto::ReplicationStatus::QUEUED:
                if (_keepTracking) wait(lock);
                else               finish(lock,
                                          SERVER_QUEUED);
                break;

            case proto::ReplicationStatus::IN_PROGRESS:
                if (_keepTracking) wait(lock);
                else               finish(lock,
                                          SERVER_IN_PROGRESS);
                break;

            case proto::ReplicationStatus::IS_CANCELLING:
                if (_keepTracking) wait(lock);
                else               finish(lock,
                                          SERVER_IS_CANCELLING);
                break;

            case proto::ReplicationStatus::BAD:
                finish(lock,
                       SERVER_BAD);
                break;

            case proto::ReplicationStatus::FAILED:
                finish(lock,
                       SERVER_ERROR);
                break;

            case proto::ReplicationStatus::CANCELLED:
                finish(lock,
                       SERVER_CANCELLED);
                break;

            default:
                throw std::logic_error(
                        "FindRequest::analyze() unknown status '" +
                        proto::ReplicationStatus_Name(message.status()) + "' received from server");
        }

    } else {
        finish(lock,
               CLIENT_ERROR);
    }
    if (_state == State::FINISHED) notify();
}

void FindRequest::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<FindRequest>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void FindRequest::savePersistentState() {
    _controller->serviceProvider()->databaseServices()->saveState(*this);
}

std::string FindRequest::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              database(),
                              chunk());
}

}}} // namespace lsst::qserv::replica
