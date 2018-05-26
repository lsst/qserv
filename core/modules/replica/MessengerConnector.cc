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
#include "replica/MessengerConnector.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MessengerConnector");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string MessengerConnector::state2string(MessengerConnector::State state) {
    switch (state) {
        case STATE_INITIAL:       return "STATE_INITIAL";
        case STATE_CONNECTING:    return "STATE_CONNECTING";
        case STATE_COMMUNICATING: return "STATE_COMMUNICATING";
    }
    throw std::logic_error(
            "incomplete implementation of method MessengerConnector::state2string()");
}

MessengerConnector::Ptr MessengerConnector::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker) {
    return MessengerConnector::Ptr(
        new MessengerConnector(serviceProvider,
                               io_service,
                               worker));
}

MessengerConnector::MessengerConnector(ServiceProvider::Ptr const&serviceProvider,
                                       boost::asio::io_service& io_service,
                                       std::string const& worker)
    :   _serviceProvider(serviceProvider),
        _workerInfo(serviceProvider->config()->workerInfo(worker)),
        _bufferCapacityBytes(serviceProvider->config()->requestBufferSizeBytes()),
        _timerIvalSec(serviceProvider->config()->retryTimeoutSec()),
        _state(State::STATE_INITIAL),
        _resolver(io_service),
        _socket(io_service),
        _timer(io_service),
        _inBuffer(serviceProvider->config()->requestBufferSizeBytes()) {
}

void MessengerConnector::stop() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "stop");

    util::Lock lock(_mtx, context() + "stop");

    // Cancel any asynchronous operation(s) if not in the initial state

    switch (_state) {

        case STATE_INITIAL:
            break;

        case STATE_CONNECTING:
        case STATE_COMMUNICATING:
            _resolver.cancel();
            _socket.cancel();
            _socket.close();
            _timer.cancel();

            _state = STATE_INITIAL;
            break;
    }
}

void MessengerConnector::cancel(std::string const& id) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel  id=" << id);

    util::Lock lock(_mtx, context() + "cancel");

    if (not _id2request.count(id)) {
        throw std::logic_error(
                "MessengerConnector::cancel()  unknow request id: " + id);
    }

    // If the request is already being processed then a more aggressive approach
    // of aborting all communications must be taken.
    if (_currentRequest and (_currentRequest->id == id)) {
        _currentRequest = nullptr;
        if (_state == STATE_COMMUNICATING) restart(lock);
    }

    // Also remove from both collections
    _requests.remove_if(
        [&id] (WrapperBase::Ptr const& ptr) {
            return ptr->id == id;
        }
    );
    _id2request.erase(id);
}

bool MessengerConnector::exists(std::string const& id) const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "exists  id=" << id);

    util::Lock lock(_mtx, context() + "exists");

    return _id2request.count(id);
}

void MessengerConnector::sendImpl(std::string const& id,
                                  MessengerConnector::WrapperBase::Ptr const& ptr) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendImpl  id: " + id);

    util::Lock lock(_mtx, context() + "sendImpl");

    if (_id2request.count(id)) {
        throw std::logic_error(
                "MessengerConnector::sendImpl()  the request is alrady registered for id:" + id);
    }

    // Register the request

    _id2request[id] = ptr;
    _requests.push_back(ptr);

    switch (_state) {

        case STATE_INITIAL:
            resolve(lock);
            break;

        case STATE_CONNECTING:
            // Not ready to submit any requests before a connection
            // is established.
            break;

        case STATE_COMMUNICATING:
            sendRequest(lock);
            break;
    }
}

void MessengerConnector::restart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    // Cancel any asynchronous operation(s) if not in the initial state

    switch (_state) {

        case STATE_INITIAL:
            break;

        case STATE_CONNECTING:
        case STATE_COMMUNICATING:
            _resolver.cancel();
            _socket.cancel();
            _socket.close();
            _timer.cancel();

            _state = STATE_INITIAL;
            break;
    }
    resolve(lock);
}

void MessengerConnector::resolve(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolve"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    if (_state != STATE_INITIAL) return;

    boost::asio::ip::tcp::resolver::query query(
        _workerInfo.svcHost,
        std::to_string(_workerInfo.svcPort)
    );
    _resolver.async_resolve(
        query,
        boost::bind(
            &MessengerConnector::resolved,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
    _state = STATE_CONNECTING;
}

void MessengerConnector::resolved(boost::system::error_code const& ec,
                                  boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolved"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    if (isAborted(ec)) return;

    util::Lock lock(_mtx, context() + "resolved");

    if (ec) waitBeforeRestart(lock);
    else    connect(lock,
                    iter);
}

void MessengerConnector::connect(util::Lock const& lock,
                                 boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connect"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    boost::asio::async_connect(
        _socket,
        iter,
        boost::bind(
            &MessengerConnector::connected,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
}

void MessengerConnector::connected(boost::system::error_code const& ec,
                                   boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connected"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    if (isAborted(ec)) return;

    util::Lock lock(_mtx, context() + "connected");

    if (ec) {
        waitBeforeRestart(lock);
    } else {
        _state = STATE_COMMUNICATING;
        sendRequest(lock);
    }
}

void MessengerConnector::waitBeforeRestart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "waitBeforeRestart"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    // Allways need to set the interval before launching the timer.

    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(
        boost::bind(
            &MessengerConnector::awakenForRestart,
            shared_from_this(),
            boost::asio::placeholders::error
        )
    );
}

void MessengerConnector::awakenForRestart(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awakenForRestart"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    if (isAborted(ec)) return;

    util::Lock lock(_mtx, context() + "awakenForRestart");

    if (_state != STATE_CONNECTING) return;

    restart(lock);
}

void MessengerConnector::sendRequest(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendRequest"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    // Check if there is an outstanding send request
    if (_currentRequest) return;

    // Pull a request (if any) from the from of the queue
    //
    if (_requests.empty()) return;
    _currentRequest = _requests.front();

    // Remove request from the front. But leave it in the map
    // as a safeguard preventing requsts with the same 'id' from being
    // attempted.
    _requests.pop_front();

    // Send the message
    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _currentRequest->requestBufferPtr->data(),
            _currentRequest->requestBufferPtr->size()
        ),
        boost::bind(
            &MessengerConnector::requestSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void MessengerConnector::requestSent(boost::system::error_code const& ec,
                                     size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    util::Lock lock(_mtx, context() + "requestSent");

    // Check if the request was cancelled while still in flight.
    // If that happens then _currentRequest should already be nullified
    // and request removed from all data structures.

    if (isAborted(ec)) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent  isAborted -> restart");

        restart(lock);
        return;
    }
    if (_currentRequest) {

        // The requst is still valid
        if (ec) {

            // If something bad happened along the line then make sure this request
            // will be the first to be served before restarting the communication.

            _requests.push_front(_currentRequest);
            _currentRequest = nullptr;

            LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent  request is valid, but failed -> restart");

            restart(lock);

        } else {

            // Go wait for a server respone

            receiveResponse(lock);
        }

    } else {
        LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent  no current request (cancelled?)");
    }
}

void MessengerConnector::receiveResponse(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveResponse"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : ""));

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    size_t const bytes = sizeof(uint32_t);
    _inBuffer.resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(
            _inBuffer.data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &MessengerConnector::responseReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void MessengerConnector::responseReceived(boost::system::error_code const& ec,
                                          size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : "")
         << " error_code=" << ec);

    // The notification if any should be happening outside the lock guard
    // to prevent deadlocks
    //
    // FIXME: this may be reconsidered because some requests may take
    // substantial amount of time to process the notification. This may
    // result in blocking processing other high-priority requests waiting
    // in the input queue. The simplest approach would be probably launch
    // the notification in a separate (new) thread.

    WrapperBase::Ptr request2notify;
    {
        util::Lock lock(_mtx, context() + "responseReceived");

        // Check if the request was cancelled while still in flight.
        // If that happens then _currentRequest should already be nullified
        // and request removed from all data structures.

        if (isAborted(ec)) {
            restart(lock);
            return;
        }

        if (_currentRequest) {

            // The requst is still valid
            if (ec) {

                // If something bad happened along the line then we should notify a caller about
                // a problem with the request, remove the request from all data structures and
                // restart the communication.

                request2notify = _currentRequest;

                _id2request.erase(_currentRequest->id);
                _currentRequest = nullptr;

                restart(lock);

            } else {

                // Receive response header into the temporary buffer

                if (syncReadVerifyHeader(lock,
                                         _inBuffer,
                                         _inBuffer.parseLength(),
                                         _currentRequest->id)) {

                    // If something bad happened along the line then we should notify a caller about
                    // a problem with the request, remove the request from all data structures and
                    // restart the communication.

                    request2notify = _currentRequest;

                    _id2request.erase(_currentRequest->id);
                    _currentRequest = nullptr;

                    restart(lock);

                } else {

                    size_t bytes;
                    if (syncReadFrame(lock,
                                      _inBuffer,
                                      bytes)) {

                        // If something bad happened along the line then we should notify a caller about
                        // a problem with the request, remove the request from all data structures and
                        // restart the communication.

                        request2notify = _currentRequest;

                        _id2request.erase(_currentRequest->id);
                        _currentRequest = nullptr;

                        restart(lock);

                    } else {

                        LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived"
                             << "  _currentRequest->id=" << _currentRequest->id
                             << " bytes=" << bytes);

                        // Then receive response body into a buffer inside the wrapper

                        if (syncReadMessageImpl(lock,
                                                _currentRequest->responseBuffer,
                                                bytes)) {

                            // If something bad happened along the line then we should notify a caller about
                            // a problem with the request, remove the request from all data structures and
                            // restart the communication.

                            request2notify = _currentRequest;

                            _id2request.erase(_currentRequest->id);
                            _currentRequest = nullptr;

                            restart(lock);

                        } else {

                            // Finally, success!

                            // Notify a caller about the successfull completion of the operation,
                            // remove this request from all data structures and initiate a new request.

                            request2notify = _currentRequest;
                            request2notify->success = true;

                            _id2request.erase(_currentRequest->id);
                            _currentRequest = nullptr;

                            sendRequest(lock);
                        }
                    }
                }
            }

        } else {

            // The request submission had a chance to finish (successfully or not)
            // before the cancellation attempt was made. So, we just forget about
            // this request and send no notification to a caller.
            //
            if (ec) restart(lock);
            else    sendRequest(lock);
        }
    }

    // Sending notifications (if requsted) outsize the lock guard

    if (request2notify) request2notify->parseAndNotify();
}

boost::system::error_code MessengerConnector::syncReadFrame(util::Lock const& lock,
                                                            replica::ProtocolBuffer& buf,
                                                            size_t& bytes) {
    size_t const frameLength = sizeof(uint32_t);
    buf.resize(frameLength);

    boost::system::error_code ec;
    boost::asio::read(
        _socket,
        boost::asio::buffer(
            buf.data(),
            frameLength
        ),
        boost::asio::transfer_at_least(frameLength),
        ec
    );
    LOGS(_log, LOG_LVL_DEBUG, context() << "syncReadFrame"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : "")
         << " error_code=" << ec);

    if (not ec) bytes = buf.parseLength();
    return ec;
}

boost::system::error_code MessengerConnector::syncReadVerifyHeader(util::Lock const& lock,
                                                                   replica::ProtocolBuffer& buf,
                                                                   size_t bytes,
                                                                   std::string const& id) {

    boost::system::error_code const ec =
        syncReadMessageImpl(lock,
                            buf,
                            bytes);
    if (not ec) {
        proto::ReplicationResponseHeader hdr;
        buf.parse(hdr, bytes);
        if (id != hdr.id()) {
            throw std::logic_error(
                    "MessengerConnector::syncReadVerifyHeader()  got unexpected id: " + hdr.id() +
                    " instead of: " + id);
        }
    }
    return ec;
}

boost::system::error_code MessengerConnector::syncReadMessageImpl(util::Lock const& lock,
                                                                  replica::ProtocolBuffer& buf,
                                                                  size_t bytes) {
    buf.resize(bytes);

    boost::system::error_code ec;
    boost::asio::read(
        _socket,
        boost::asio::buffer(
            buf.data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    LOGS(_log, LOG_LVL_DEBUG, context() << "syncReadMessageImpl"
         << "  _currentRequest->id=" << (_currentRequest ? _currentRequest->id : "")
         << " error_code=" << ec);

    return ec;
}

bool MessengerConnector::isAborted(boost::system::error_code const& ec) const {

    if (ec == boost::asio::error::operation_aborted) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "isAborted  ** ABORTED **");
        return true;
    }
    return false;
}

std::string MessengerConnector::context() const {
    return "MESSENGER-CONNECTION [worker=" + _workerInfo.name + ", state=" + state2string(_state) + "]  ";
}

}}} // namespace lsst::qserv::replica
