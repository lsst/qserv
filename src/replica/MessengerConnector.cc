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
#include "replica/MessengerConnector.h"

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MessengerConnector");

string ec2str(boost::system::error_code const& ec) {
    return ec.category().name() + string(":") + to_string(ec.value()) + "[" + ec.message() +"]";
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string MessengerConnector::_state2string(MessengerConnector::State state) {
    switch (state) {
        case STATE_INITIAL:       return "STATE_INITIAL";
        case STATE_CONNECTING:    return "STATE_CONNECTING";
        case STATE_COMMUNICATING: return "STATE_COMMUNICATING";
    }
    throw logic_error(
            "MessengerConnector::" + string(__func__) + "  incomplete implementation");
}


MessengerConnector::Ptr MessengerConnector::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        string const& worker) {
    return MessengerConnector::Ptr(
            new MessengerConnector(serviceProvider, io_service, worker));
}


MessengerConnector::MessengerConnector(ServiceProvider::Ptr const& serviceProvider,
                                       boost::asio::io_service& io_service,
                                       string const& worker)
    :   _serviceProvider(serviceProvider),
        _workerInfo(serviceProvider->config()->workerInfo(worker)),
        _bufferCapacityBytes(serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes")),
        _timerIvalSec(serviceProvider->config()->get<unsigned int>("common", "request-retry-interval-sec")),
        _state(State::STATE_INITIAL),
        _resolver(io_service),
        _socket(io_service),
        _timer(io_service),
        _inBuffer(serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes")) {
}


void MessengerConnector::stop() {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    list<MessageWrapperBase::Ptr> requests2notify;
    {
        util::Lock lock(_mtx, _context() + __func__);

        // The error code is used to call non-throwing methods and to prevent exceptions.
        // Note that it makes no sense to handle (not even report) any errors within the body
        // of this method.
        boost::system::error_code ec;

        // Cancel any asynchronous operation(s) if not in the initial state
        switch (_state) {
    
            case STATE_INITIAL:
                break;
    
            case STATE_CONNECTING:
            case STATE_COMMUNICATING:
                _resolver.cancel();
                if (_state == STATE_COMMUNICATING) {
                    _socket.cancel(ec);
                    _socket.close(ec);
                }
                _timer.cancel(ec);
                _state = STATE_INITIAL;
   
                // Make sure the current request's owner gets notified
                if (_currentRequest != nullptr) {
                    requests2notify.push_back(_currentRequest);
                    _currentRequest = nullptr;
                }

                // Also cancel the queued requests and notify their owners
                while (true) {
                    auto const ptr = _requests.front();
                    if (ptr == nullptr) break;
                    requests2notify.push_back(ptr);
                }
                break;
    
            default:
                throw logic_error(
                        "MessengerConnector::" + string(__func__) + "  incomplete implementation");
        }
    }

    // Sending notifications (if requested) outsize the lock guard to avoid deadlocks.
    for (auto&& ptr: requests2notify) {
        ptr->parseAndNotify();
    }
}


void MessengerConnector::cancel(string const& id) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  id=" << id);

    // Remove request from the queue (if it's still there)
    _requests.remove(id);

    // Also, if the request is already being processed then terminate all
    // communications with a worker.
    if (_state == STATE_COMMUNICATING) {
        if ((_currentRequest != nullptr) and (_currentRequest->id() == id)) {
            // Make sure the request gets eliminated before restarting the connection.
            // Otherwise it will be picked up again.
            _currentRequest = nullptr;
            _restart(lock);
        }
    }
}


bool MessengerConnector::exists(string const& id) const {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  id=" << id);
    return _requests.find(id) != nullptr;
}


void MessengerConnector::_sendImpl(MessageWrapperBase::Ptr const& ptr) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  id=" << ptr->id());

    if (_requests.find(ptr->id()) != nullptr) {
        throw logic_error(
                "MessengerConnector::" + string(__func__)
                + "  the request is already registered for id:" + ptr->id());
    }

    // Register the request
    _requests.push_back(ptr);

    switch (_state) {

        case STATE_INITIAL:
            _resolve(lock);
            break;

        case STATE_CONNECTING:
            // Not ready to submit any requests before a connection
            // is established.
            break;

        case STATE_COMMUNICATING:
            _sendRequest(lock);
            break;

        default:
            throw logic_error(
                    "MessengerConnector::" + string(__func__) + "  incomplete implementation");
    }
}


void MessengerConnector::_restart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    // The error code is used to call non-throwing methods and to prevent exceptions.
    // Note that it makes no sense to handle (not even report) any errors within the body
    // of this method.
    boost::system::error_code ec;

    // Cancel any asynchronous operation(s) if not in the initial state
    switch (_state) {
        case STATE_INITIAL: break;
        case STATE_CONNECTING:
        case STATE_COMMUNICATING:
            _resolver.cancel();
            if (_state == STATE_COMMUNICATING) {
                // Save curent request into the front of the queue, so that it would be the first
                // one to be processed upon successful completion of the restart.
                if (_currentRequest != nullptr) {
                    _requests.push_front(_currentRequest);
                    _currentRequest = nullptr;
                }
                _socket.cancel(ec);
                _socket.close(ec);
            }
            _timer.cancel(ec);
            _state = STATE_INITIAL;
            break;

        default:
            throw logic_error(
                    "MessengerConnector::" + string(__func__) + "  incomplete implementation");
    }
    _resolve(lock);
}


void MessengerConnector::_resolve(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    if (_state != STATE_INITIAL) return;

    boost::asio::ip::tcp::resolver::query query(_workerInfo.svcHost, to_string(_workerInfo.svcPort));
    _resolver.async_resolve(query, bind(&MessengerConnector::_resolved,
                            shared_from_this(), _1, _2));
    _state = STATE_CONNECTING;
}


void MessengerConnector::_resolved(boost::system::error_code const& ec,
                                   boost::asio::ip::tcp::resolver::iterator iter) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  ec=" << ec2str(ec));

    if (_state != STATE_CONNECTING) return;

    if (_failed(ec)) {
        _waitBeforeRestart(lock);
    } else {
        _connect(lock, iter);
    }
}


void MessengerConnector::_connect(util::Lock const& lock,
                                  boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    boost::asio::async_connect(
            _socket, iter, bind(&MessengerConnector::_connected, shared_from_this(), _1, _2));
}


void MessengerConnector::_connected(boost::system::error_code const& ec,
                                    boost::asio::ip::tcp::resolver::iterator iter) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  ec=" << ec2str(ec));

    if (_state != STATE_CONNECTING) return;

    if (_failed(ec)) {
        _waitBeforeRestart(lock);
    } else {
        _state = STATE_COMMUNICATING;
        _sendRequest(lock);
    }
}


void MessengerConnector::_waitBeforeRestart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    // Always need to set the interval before launching the timer.
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait(bind(&MessengerConnector::_awakenForRestart, shared_from_this(),_1));
}


void MessengerConnector::_awakenForRestart(boost::system::error_code const& ec) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  ec=" << ec2str(ec));

    if (_state != STATE_CONNECTING) return;

    // The timer was explicitly aborted by the connection restart method. So, don't
    // bother with doing anything here.
    if (ec == boost::asio::error::operation_aborted) return;

    // In case of the normal expiration of the timer try restarting the connection.
    if (ec.value() == 0) {
        _restart(lock);
        return;
    }

    // This is an abnormal scenario since timer should not arbitrary fail with
    // any other error code.
    throw runtime_error("MessengerConnector::" + string(__func__) + "  error: " + ec2str(ec));
}


void MessengerConnector::_sendRequest(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << ":1"
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    if (_state != STATE_COMMUNICATING) return;

    // Check if there is another request in flight.
    if (_currentRequest) return;

    // Pull the next available request (if any) from the queue.
    if (_requests.empty()) return;
    _currentRequest = _requests.front();

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << ":1"
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    boost::asio::async_write(_socket,
            boost::asio::buffer(_currentRequest->requestBufferPtr()->data(),
                                _currentRequest->requestBufferPtr()->size()),
            bind(&MessengerConnector::_requestSent, shared_from_this(), _1, _2));
}


void MessengerConnector::_requestSent(boost::system::error_code const& ec,
                                      size_t bytes_transferred) {

    util::Lock lock(_mtx, _context() + __func__);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << "  ec=" << ec2str(ec));

    if (_state != STATE_COMMUNICATING) return;

    // Check if the request was cancelled. If so then pull another one (if any) from
    // the queue.
    if (_currentRequest == nullptr) {
        _sendRequest(lock);
        return;
    }

    // The request will be retried later after restarting the connection.
    if (_failed(ec)) {
        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  ** FAILED **");
        _restart(lock);
        return;
    }

    // Go wait for a server response
    _receiveResponse(lock);
}


void MessengerConnector::_receiveResponse(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size());

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whole message (its frame and
    // the message itself) at once.

    size_t const bytes = sizeof(uint32_t);
    _inBuffer.resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_inBuffer.data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&MessengerConnector::_responseReceived, shared_from_this(), _1, _2));
}


void MessengerConnector::_responseReceived(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    // The notification if any should be happening outside the lock guard
    // to prevent deadlocks
    //
    // FIXME: this may be reconsidered because some requests may take
    // substantial amount of time to process the notification. This may
    // result in blocking processing other high-priority requests waiting
    // in the input queue. The simplest approach would be probably launch
    // the notification in a separate (new) thread.

    MessageWrapperBase::Ptr request2notify;
    {
        util::Lock lock(_mtx, _context() + __func__);

        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
             << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
             << "  _requests.size=" << _requests.size()
             << "  ec=" << ec2str(ec));

        if (_state != STATE_COMMUNICATING) return;

        // Check if the request was cancelled while still in flight.
        if (_currentRequest == nullptr) {
            _restart(lock);
            return;
        }

        // At this point we're done with the current request, regardless of its completion
        // status, or any failures to pull or digest the response data. Hence, removing
        // completely it and getting ready to notify a caller.
        //
        // NOTE: by default a request would be marked as failed, unless the following method
        // is called: request2notify->setSuccess(true).

        swap(request2notify, _currentRequest);

        if (_failed(ec)) {
            // Failed to get any response from a worker. The connection is in an unusable state,
            // and it needs to be reset.
            _restart(lock);
        } else {

            // Receive response header into the temporary buffer.
            if (0 != _syncReadVerifyHeader(lock,
                                           _inBuffer,
                                           _inBuffer.parseLength(),
                                           request2notify->id()).value()) {
                // Failed to receive the header
                _restart(lock);
            } else {
                // Read the response frame
                size_t bytes;
                if (0 != _syncReadFrame(lock, _inBuffer, bytes).value()) {
                    // Failed to read the frame
                    _restart(lock);
                } else {
                    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
                         << "  _currentRequest=" << request2notify->id() << " bytes=" << bytes);

                    // Receive response body into a buffer inside the wrapper
                    if (0 != _syncReadMessageImpl(lock,
                                                  request2notify->responseBuffer(),
                                                  bytes).value()) {
                        // Failed to read the message body
                        _restart(lock);
                    } else {
                        // Finally, success!
                        request2notify->setSuccess(true);

                        // Initiate the next request (if any) processing
                        _sendRequest(lock);
                    }
                }
            }
        }
    }

    // Sending notifications (if requested) outsize the lock guard to avoid
    // deadlocks.
    if (request2notify) request2notify->parseAndNotify();
}


boost::system::error_code MessengerConnector::_syncReadFrame(util::Lock const& lock,
                                                             ProtocolBuffer& buf,
                                                             size_t& bytes) {
    size_t const frameLength = sizeof(uint32_t);
    buf.resize(frameLength);

    boost::system::error_code ec;
    boost::asio::read(
        _socket,
        boost::asio::buffer(buf.data(), frameLength),
        boost::asio::transfer_at_least(frameLength),
        ec);
    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << " ec=" << ec);

    if (ec.value() == 0) bytes = buf.parseLength();
    return ec;
}


boost::system::error_code MessengerConnector::_syncReadVerifyHeader(util::Lock const& lock,
                                                                    ProtocolBuffer& buf,
                                                                    size_t bytes,
                                                                    string const& id) {
    boost::system::error_code const ec = _syncReadMessageImpl(lock, buf, bytes);
    if (ec.value() == 0) {
        ProtocolResponseHeader hdr;
        buf.parse(hdr, bytes);
        if (id != hdr.id()) {
            throw logic_error(
                    "MessengerConnector::" + string(__func__) + "  got unexpected id: " + hdr.id() +
                    " instead of: " + id);
        }
    }
    return ec;
}


boost::system::error_code MessengerConnector::_syncReadMessageImpl(util::Lock const& lock,
                                                                   ProtocolBuffer& buf,
                                                                   size_t bytes) {
    buf.resize(bytes);
    boost::system::error_code ec;
    boost::asio::read(
        _socket,
        boost::asio::buffer(buf.data(), bytes),
        boost::asio::transfer_at_least(bytes),
        ec);

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
         << "  _currentRequest=" << (_currentRequest ? _currentRequest->id() : "")
         << "  _requests.size=" << _requests.size()
         << " ec=" << ec);

    return ec;
}


bool MessengerConnector::_failed(boost::system::error_code const& ec) const {
    if (ec.value() != 0) {
        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  ** FAILED **  ec=" << ec2str(ec));
        return true;
    }
    return false;
}


string MessengerConnector::_context() const {
    return "MESSENGER-CONNECTION [worker=" + _workerInfo.name + ", state=" +
            _state2string(_state) + "]  ";
}

}}} // namespace lsst::qserv::replica
