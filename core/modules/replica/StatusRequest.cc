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
#include "replica/StatusRequest.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.StatusRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

/////////////////////////////////////////
//         StatusRequestBaseC          //
/////////////////////////////////////////

StatusRequestBaseC::StatusRequestBaseC (ServiceProvider&                                  serviceProvider,
                                        boost::asio::io_service&                          io_service,
                                        char const*                                       requestTypeName,
                                        std::string const&                                worker,
                                        std::string const&                                targetRequestId,
                                        lsst::qserv::proto::ReplicationReplicaRequestType requestType,
                                        bool                                              keepTracking)

    :   RequestConnection (serviceProvider,
                           io_service,
                           requestTypeName,
                           worker,
                           0,   /* priority */
                           keepTracking,
                           false /* allowDuplicate */),

        _targetRequestId (targetRequestId),
        _requestType     (requestType) {
}

void
StatusRequestBaseC::beginProtocol () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProtocol");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id             (id());
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id  (_targetRequestId);
    message.set_type(_requestType);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &StatusRequestBaseC::requestSent,
            shared_from_base<StatusRequestBaseC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequestBaseC::requestSent (boost::system::error_code const& ec,
                                 size_t                           bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
StatusRequestBaseC::receiveResponse () {

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
            &StatusRequestBaseC::responseReceived,
            shared_from_base<StatusRequestBaseC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequestBaseC::responseReceived (boost::system::error_code const& ec,
                                      size_t                           bytes_transferred) {

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
           
    if (syncReadMessageImpl (bytes)) restart();
    else                             analyze(parseResponse());
}

void
StatusRequestBaseC::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &StatusRequestBaseC::awaken,
            shared_from_base<StatusRequestBaseC>(),
            boost::asio::placeholders::error
        )
    );
}

void
StatusRequestBaseC::awaken (boost::system::error_code const& ec) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    sendStatus();
}

void
StatusRequestBaseC::sendStatus () {

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
    message.set_id  (_targetRequestId);
    message.set_type(_requestType);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &StatusRequestBaseC::statusSent,
            shared_from_base<StatusRequestBaseC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequestBaseC::statusSent (boost::system::error_code const& ec,
                                size_t                           bytes_transferred) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveStatus();
}

void
StatusRequestBaseC::receiveStatus () {

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
            &StatusRequestBaseC::statusReceived,
            shared_from_base<StatusRequestBaseC>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequestBaseC::statusReceived (boost::system::error_code const& ec,
                                    size_t                           bytes_transferred) {

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
           
    if (syncReadMessageImpl (bytes)) restart();
    else                             analyze(parseResponse());
}

void
StatusRequestBaseC::analyze (proto::ReplicationStatus status) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  remote status: " << proto::ReplicationStatus_Name(status));

    switch (status) {
 
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
                    "StatusRequestBaseC::analyze() unknown status '" + proto::ReplicationStatus_Name(status) +
                    "' received from server");
    }
}

/////////////////////////////////////////
//         StatusRequestBaseM          //
/////////////////////////////////////////

StatusRequestBaseM::StatusRequestBaseM (ServiceProvider&                                  serviceProvider,
                                        boost::asio::io_service&                          io_service,
                                        char const*                                       requestTypeName,
                                        std::string const&                                worker,
                                        std::string const&                                targetRequestId,
                                        lsst::qserv::proto::ReplicationReplicaRequestType requestType,
                                        bool                                              keepTracking,
                                        std::shared_ptr<Messenger> const&                 messenger)

    :   RequestMessenger (serviceProvider,
                          io_service,
                          requestTypeName,
                          worker,
                          0,    /* priority */
                          keepTracking,
                          false /* allowDuplicate */,
                          messenger),

        _targetRequestId (targetRequestId),
        _requestType     (requestType) {
}

void
StatusRequestBaseM::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id             (id());
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id  (_targetRequestId);
    message.set_type(_requestType);

    _bufferPtr->serialize(message);

    send();
}

void
StatusRequestBaseM::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &StatusRequestBaseM::awaken,
            shared_from_base<StatusRequestBaseM>(),
            boost::asio::placeholders::error
        )
    );
}

void
StatusRequestBaseM::awaken (boost::system::error_code const& ec) {

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
    message.set_id  (_targetRequestId);
    message.set_type(_requestType);

    _bufferPtr->serialize(message);

    send();
}

void
StatusRequestBaseM::analyze (bool                     success,
                             proto::ReplicationStatus status) {

    // This guard is made on behalf of an asynchronious callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze() 
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze");

    if (success) {

        switch (status) {
     
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
                        "StatusRequestBaseM::analyze() unknown status '" + proto::ReplicationStatus_Name(status) +
                        "' received from server");
        }

    } else {
        finish (CLIENT_ERROR);
    }
}

}}} // namespace lsst::qserv::replica
