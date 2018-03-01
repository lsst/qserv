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

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {


//////////////////////////////////////
//         FindAllRequestC          //
//////////////////////////////////////

FindAllRequestC::pointer
FindAllRequestC::create (ServiceProvider&        serviceProvider,
                        boost::asio::io_service& io_service,
                        std::string const&       worker,
                        std::string const&       database,
                        callback_type            onFinish,
                        int                      priority,
                        bool                     keepTracking) {

    return FindAllRequestC::pointer (
        new FindAllRequestC (
            serviceProvider,
            io_service,
            worker,
            database,
            onFinish,
            priority,
            keepTracking));
}

FindAllRequestC::FindAllRequestC (ServiceProvider&         serviceProvider,
                                  boost::asio::io_service& io_service,
                                  std::string const&       worker,
                                  std::string const&       database,
                                  callback_type            onFinish,
                                  int                      priority,
                                  bool                     keepTracking)
    :   RequestConnection (serviceProvider,
                           io_service,
                           "REPLICA_FIND_ALL",
                           worker,
                           priority,
                           keepTracking,
                           false /* allowDuplicate */),
        _database              (database),
        _onFinish              (onFinish),
        _replicaInfoCollection () {

    _serviceProvider.assertDatabaseIsValid (database);
}

ReplicaInfoCollection const&
FindAllRequestC::responseData () const {
    return _replicaInfoCollection;
}

void
FindAllRequestC::beginProtocol () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProtocol");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id          (id());
    hdr.set_type        (proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestFindAll message;
    message.set_priority  (priority());
    message.set_database  (database());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &FindAllRequestC::requestSent,
            shared_from_base<FindAllRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequestC::requestSent (boost::system::error_code const& ec,
                              size_t bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
FindAllRequestC::receiveResponse () {

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

    boost::asio::async_read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &FindAllRequestC::responseReceived,
            shared_from_base<FindAllRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequestC::responseReceived (boost::system::error_code const& ec,
                                   size_t bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader (_bufferPtr->parseLength())) restart();
    
    size_t bytes;
    if (syncReadFrame (bytes)) restart ();
           
    proto::ReplicationResponseFindAll message;
    if (syncReadMessage (bytes, message)) restart();
    else                                  analyze(message);
}

void
FindAllRequestC::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &FindAllRequestC::awaken,
            shared_from_base<FindAllRequestC>(),
            boost::asio::placeholders::error
        )
    );
}

void
FindAllRequestC::awaken (boost::system::error_code const& ec) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    sendStatus();
}

void
FindAllRequestC::sendStatus () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendStatus");

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id             (id());
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id  (id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &FindAllRequestC::statusSent,
            shared_from_base<FindAllRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequestC::statusSent (boost::system::error_code const& ec,
                             size_t bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveStatus();
}

void
FindAllRequestC::receiveStatus () {

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

    boost::asio::async_read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &FindAllRequestC::statusReceived,
            shared_from_base<FindAllRequestC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequestC::statusReceived (boost::system::error_code const& ec,
                                 size_t bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader (_bufferPtr->parseLength())) restart();
    
    size_t bytes;
    if (syncReadFrame (bytes)) restart ();
           
    proto::ReplicationResponseFindAll message;
    if (syncReadMessage (bytes, message)) restart();
    else                                  analyze(message);
}

void
FindAllRequestC::analyze (proto::ReplicationResponseFindAll const& message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  remote status: "
         << proto::ReplicationStatus_Name(message.status()));

    // Always get the latest status reported by the remote server
    _extendedServerStatus = replica::translate(message.status_ext());

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (message.has_target_performance())
        _performance.update(message.target_performance());
    else
        _performance.update(message.performance());

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    for (int num = message.replica_info_many_size(), idx = 0; idx < num; ++idx)
        _replicaInfoCollection.emplace_back(&(message.replica_info_many(idx)));

    // Extract target request type-specific parameters from the response
    if (message.has_request())
        _targetRequestParams = FindAllRequestParams(message.request());

    switch (message.status()) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish (SUCCESS);
            break;

        case proto::ReplicationStatus::QUEUED:
            if (_keepTracking) wait();
            else               finish (SERVER_QUEUED);
            break;

        case proto::ReplicationStatus::IN_PROGRESS:
            if (_keepTracking) wait();
            else               finish (SERVER_IN_PROGRESS);
            break;

        case proto::ReplicationStatus::IS_CANCELLING:
            if (_keepTracking) wait();
            else               finish (SERVER_IS_CANCELLING);
            break;

        case proto::ReplicationStatus::BAD:
            finish (SERVER_BAD);
            break;

        case proto::ReplicationStatus::FAILED:
            finish (SERVER_ERROR);
            break;

        case proto::ReplicationStatus::CANCELLED:
            finish (SERVER_CANCELLED);
            break;

        default:
            throw std::logic_error (
                    "FindAllRequestC::analyze() unknown status '" +
                    proto::ReplicationStatus_Name(message.status()) + "' received from server");

    }
}

void
FindAllRequestC::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<FindAllRequestC>());
    }
}

//////////////////////////////////////
//         FindAllRequestM          //
//////////////////////////////////////

FindAllRequestM::pointer
FindAllRequestM::create (ServiceProvider&                  serviceProvider,
                         boost::asio::io_service&          io_service,
                         std::string const&                worker,
                         std::string const&                database,
                         callback_type                     onFinish,
                         int                               priority,
                         bool                              keepTracking,
                         std::shared_ptr<Messenger> const& messenger) {
    return FindAllRequestM::pointer (
        new FindAllRequestM (
            serviceProvider,
            io_service,
            worker,
            database,
            onFinish,
            priority,
            keepTracking,
            messenger));
}

FindAllRequestM::FindAllRequestM (ServiceProvider&                  serviceProvider,
                                  boost::asio::io_service&          io_service,
                                  std::string const&                worker,
                                  std::string const&                database,
                                  callback_type                     onFinish,
                                  int                               priority,
                                  bool                              keepTracking,
                                  std::shared_ptr<Messenger> const& messenger)
    :   RequestMessenger (serviceProvider,
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

    _serviceProvider.assertDatabaseIsValid (database);
}

const ReplicaInfoCollection&
FindAllRequestM::responseData () const {
    return _replicaInfoCollection;
}

void
FindAllRequestM::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id          (id());
    hdr.set_type        (proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestFindAll message;
    message.set_priority  (priority());
    message.set_database  (database());

    _bufferPtr->serialize(message);

    send();
}

void
FindAllRequestM::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &FindAllRequestM::awaken,
            shared_from_base<FindAllRequestM>(),
            boost::asio::placeholders::error
        )
    );
}

void
FindAllRequestM::awaken (boost::system::error_code const& ec) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id             (id());
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id  (id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(message);

    // Send the message

    send();
}

void
FindAllRequestM::send () {

    auto self = shared_from_base<FindAllRequestM>();

    _messenger->send<proto::ReplicationResponseFindAll> (
        worker(),
        id(),
        _bufferPtr,
        [self] (std::string const&                       id,
                bool                                     success,
                proto::ReplicationResponseFindAll const& response) {
            self->analyze (success, response);
        }
    );
}

void
FindAllRequestM::analyze (bool success,
                          proto::ReplicationResponseFindAll const& message) {

    // This guard is made on behalf of an asynchronious callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze() 
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze");

    if (success) {

        // Always get the latest status reported by the remote server
        _extendedServerStatus = replica::translate(message.status_ext());
    
        // Performance counters are updated from either of two sources,
        // depending on the availability of the 'target' performance counters
        // filled in by the 'STATUS' queries. If the later is not available
        // then fallback to the one of the current request.
    
        if (message.has_target_performance())
            _performance.update(message.target_performance());
        else
            _performance.update(message.performance());
    
        // Always extract extended data regardless of the completion status
        // reported by the worker service.
    
        for (int num = message.replica_info_many_size(), idx = 0; idx < num; ++idx)
            _replicaInfoCollection.emplace_back(&(message.replica_info_many(idx)));

        // Extract target request type-specific parameters from the response
        if (message.has_request())
            _targetRequestParams = FindAllRequestParams(message.request());

        switch (message.status()) {
     
            case proto::ReplicationStatus::SUCCESS:
                finish (SUCCESS);
                break;
    
            case proto::ReplicationStatus::QUEUED:
                if (_keepTracking) wait();
                else               finish (SERVER_QUEUED);
                break;
    
            case proto::ReplicationStatus::IN_PROGRESS:
                if (_keepTracking) wait();
                else               finish (SERVER_IN_PROGRESS);
                break;
    
            case proto::ReplicationStatus::IS_CANCELLING:
                if (_keepTracking) wait();
                else               finish (SERVER_IS_CANCELLING);
                break;
    
            case proto::ReplicationStatus::BAD:
                finish (SERVER_BAD);
                break;
    
            case proto::ReplicationStatus::FAILED:
                finish (SERVER_ERROR);
                break;
    
            case proto::ReplicationStatus::CANCELLED:
                finish (SERVER_CANCELLED);
                break;
    
            default:
                throw std::logic_error (
                        "FindAllRequestM::analyze() unknown status '" +
                        proto::ReplicationStatus_Name(message.status()) + "' received from server");
        }

    } else {
        finish (CLIENT_ERROR);
    }
}

void
FindAllRequestM::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<FindAllRequestM>());
    }
}

}}} // namespace lsst::qserv::replica