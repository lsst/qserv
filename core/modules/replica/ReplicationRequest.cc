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
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

//////////////////////////////////////////
//         ReplicationRequestC          //
//////////////////////////////////////////

ReplicationRequestC::pointer ReplicationRequestC::create(
                                    ServiceProvider::pointer const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    callback_type onFinish,
                                    int  priority,
                                    bool keepTracking,
                                    bool allowDuplicate) {
    return ReplicationRequestC::pointer(
        new ReplicationRequestC(
            serviceProvider,
            io_service,
            worker,
            sourceWorker,
            database,
            chunk,
            onFinish,
            priority,
            keepTracking,
            allowDuplicate));
}

ReplicationRequestC::ReplicationRequestC (
                                    ServiceProvider::pointer const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    callback_type onFinish,
                                    int  priority,
                                    bool keepTracking,
                                    bool allowDuplicate)
    :   RequestConnection(serviceProvider,
                          io_service,
                          "REPLICA_CREATE",
                          worker,
                          priority,
                          keepTracking,
                          allowDuplicate),
        _database(database),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _onFinish(onFinish),
        _replicaInfo() {

    _serviceProvider->assertWorkerIsValid(sourceWorker);
    _serviceProvider->assertWorkersAreDifferent(sourceWorker, worker);
    _serviceProvider->assertDatabaseIsValid(database);
}

void ReplicationRequestC::beginProtocol() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProtocol");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestReplicate message;
    message.set_priority(priority());
    message.set_database(database());
    message.set_chunk(chunk());
    message.set_worker(sourceWorker());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind(
            &ReplicationRequestC::requestSent,
            shared_from_base<ReplicationRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void ReplicationRequestC::requestSent(boost::system::error_code const& ec,
                                      size_t bytes_transferred) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent");

    if (isAborted(ec)) { return; }

    if (ec) { restart(); }
    else    { receiveResponse(); }
}

void ReplicationRequestC::receiveResponse() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveResponse");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    size_t const bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &ReplicationRequestC::responseReceived,
            shared_from_base<ReplicationRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void ReplicationRequestC::responseReceived(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived");

    if (isAborted(ec)) { return; }

    if (ec) { restart(); return; }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader(_bufferPtr->parseLength())) { restart(); }

    size_t bytes;
    if (syncReadFrame(bytes)) { restart (); }

    proto::ReplicationResponseReplicate message;
    if (syncReadMessage(bytes, message)) { restart(); }
    else                                 { analyze(message); }
}

void ReplicationRequestC::wait() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
        boost::bind(
            &ReplicationRequestC::awaken,
            shared_from_base<ReplicationRequestC>(),
            boost::asio::placeholders::error
        )
    );
}

void ReplicationRequestC::awaken(boost::system::error_code const& ec) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) { return; }

    // Also ignore this event if the request expired
    if (_state == State::FINISHED) { return; }
    sendStatus();
}

void ReplicationRequestC::sendStatus() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendStatus");

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(remoteId());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind(
            &ReplicationRequestC::statusSent,
            shared_from_base<ReplicationRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void ReplicationRequestC::statusSent(boost::system::error_code const& ec,
                                     size_t bytes_transferred) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusSent");

    if (isAborted(ec)) { return; }

    if (ec) { restart(); }
    else    { receiveStatus(); }
}

void ReplicationRequestC::receiveStatus() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveStatus");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    size_t const bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &ReplicationRequestC::statusReceived,
            shared_from_base<ReplicationRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void ReplicationRequestC::statusReceived(boost::system::error_code const& ec,
                                         size_t bytes_transferred) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusReceived");

    if (isAborted(ec)) { return; }

    if (ec) {
        restart();
        return;
    }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader(_bufferPtr->parseLength())) { restart(); }

    size_t bytes;
    if (syncReadFrame(bytes)) { restart (); }

    proto::ReplicationResponseReplicate message;
    if (syncReadMessage(bytes, message)) { restart(); }
    else                                 { analyze(message); }
}

void ReplicationRequestC::analyze(proto::ReplicationResponseReplicate const& message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  remote status: "
         << proto::ReplicationStatus_Name(message.status()));

    // Always get the latest status reported by the remote server
    _extendedServerStatus = replica::translate(message.status_ext());

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (message.has_target_performance()) { _performance.update(message.target_performance()); }
    else                                  { _performance.update(message.performance()); }

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    _replicaInfo = ReplicaInfo(&(message.replica_info()));

    // Extract target request type-specific parameters from the response
    if (message.has_request()) {
        _targetRequestParams = ReplicationRequestParams(message.request());
    }
    switch (message.status()) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish(SUCCESS);
            break;

            case proto::ReplicationStatus::QUEUED:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_QUEUED); }
                break;
    
            case proto::ReplicationStatus::IN_PROGRESS:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_IN_PROGRESS); }
                break;
    
            case proto::ReplicationStatus::IS_CANCELLING:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_IS_CANCELLING); }
                break;
    
            case proto::ReplicationStatus::BAD:

                // Special treatment of the duplicate requests if allowed

                if (_extendedServerStatus == ExtendedCompletionStatus::EXT_STATUS_DUPLICATE) {
                    Request::_duplicateRequestId = message.duplicate_request_id();
                    if (_allowDuplicate && _keepTracking) {
                        wait();
                        return;
                    }
                }
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
                    "ReplicationRequestC::analyze() unknown status '" +
                    proto::ReplicationStatus_Name(message.status()) +
                    "' received from server");
    }
}

void ReplicationRequestC::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<ReplicationRequestC>());
    }
}

//////////////////////////////////////////
//         ReplicationRequestM          //
//////////////////////////////////////////

ReplicationRequestM::pointer ReplicationRequestM::create(
                                    ServiceProvider::pointer const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    callback_type onFinish,
                                    int  priority,
                                    bool keepTracking,
                                    bool allowDuplicate,
                                    std::shared_ptr<Messenger> const& messenger) {

    return ReplicationRequestM::pointer(
        new ReplicationRequestM(
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

ReplicationRequestM::ReplicationRequestM(
                                    ServiceProvider::pointer const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& sourceWorker,
                                    std::string const& database,
                                    unsigned int  chunk,
                                    callback_type onFinish,
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

    _serviceProvider->assertWorkerIsValid(sourceWorker);
    _serviceProvider->assertWorkersAreDifferent(sourceWorker, worker);
    _serviceProvider->assertDatabaseIsValid(database);
}

void ReplicationRequestM::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestReplicate message;
    message.set_priority(priority());
    message.set_database(database());
    message.set_chunk(chunk());
    message.set_worker(sourceWorker());

    _bufferPtr->serialize(message);

    send();
}

void ReplicationRequestM::wait() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
        boost::bind(
            &ReplicationRequestM::awaken,
            shared_from_base<ReplicationRequestM>(),
            boost::asio::placeholders::error
        )
    );
}

void ReplicationRequestM::awaken(boost::system::error_code const& ec) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) { return; }

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) { return; }

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(remoteId());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(message);

    send();
}

void ReplicationRequestM::send() {

    auto self = shared_from_base<ReplicationRequestM>();

    _messenger->send<proto::ReplicationResponseReplicate>(
        worker(),
        id(),
        _bufferPtr,
        [self] (std::string const& id,
                bool success,
                proto::ReplicationResponseReplicate const& response) {
            self->analyze (success, response);
        }
    );
}

void ReplicationRequestM::analyze(bool success,
                                  proto::ReplicationResponseReplicate const& message) {

    // This guard is made on behalf of an asynchronious callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze() 
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze");

    if (success) {

        // Allways get the latest status reported by the remote server
        _extendedServerStatus = replica::translate(message.status_ext());
    
        // Performance counters are updated from either of two sources,
        // depending on the availability of the 'target' performance counters
        // filled in by the 'STATUS' queries. If the later is not available
        // then fallback to the one of the current request.
    
        if (message.has_target_performance()) { _performance.update(message.target_performance()); }
        else                                  { _performance.update(message.performance()); }
    
        // Always extract extended data regardless of the completion status
        // reported by the worker service.
    
        _replicaInfo = ReplicaInfo(&(message.replica_info()));

        // Extract target request type-specific parameters from the response
        if (message.has_request()) {
            _targetRequestParams = ReplicationRequestParams(message.request());
        }
        switch (message.status()) {
     
            case proto::ReplicationStatus::SUCCESS:
                finish(SUCCESS);
                break;
    
            case proto::ReplicationStatus::QUEUED:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_QUEUED); }
                break;
    
            case proto::ReplicationStatus::IN_PROGRESS:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_IN_PROGRESS); }
                break;
    
            case proto::ReplicationStatus::IS_CANCELLING:
                if (_keepTracking) { wait(); }
                else               { finish(SERVER_IS_CANCELLING); }
                break;
    
            case proto::ReplicationStatus::BAD:

                // Special treatment of the duplicate requests if allowed

                if (_extendedServerStatus == ExtendedCompletionStatus::EXT_STATUS_DUPLICATE) {
                    Request::_duplicateRequestId = message.duplicate_request_id();
                    if (_allowDuplicate && _keepTracking) {
                        wait();
                        return;
                    }
                }
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
                        "ReplicationRequestM::analyze() unknown status '" +
                        proto::ReplicationStatus_Name(message.status()) +
                        "' received from server");
        }

    } else {
        finish(CLIENT_ERROR);
    }
}

void ReplicationRequestM::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<ReplicationRequestM>());
    }
}

}}} // namespace lsst::qserv::replica