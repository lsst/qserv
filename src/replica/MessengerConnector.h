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
#ifndef LSST_QSERV_REPLICA_MESSENGERCONNECTOR_H
#define LSST_QSERV_REPLICA_MESSENGERCONNECTOR_H

// System headers
#include <algorithm>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/MessageQueue.h"
#include "replica/protocol.pb.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class MessageWrapperBase is the base class for request wrappers.
 */
class MessageWrapperBase {
public:
    typedef std::shared_ptr<MessageWrapperBase> Ptr;

    MessageWrapperBase() = delete;
    MessageWrapperBase(MessageWrapperBase const&) = delete;
    MessageWrapperBase& operator=(MessageWrapperBase const&) = delete;

    virtual ~MessageWrapperBase() = default;

    /// @return The completion status to be returned to a subscriber.
    bool success() const { return _success; }

    /// @return A unique identifier of the request.
    std::string const& id() const { return _id; }

    /// @return The priority lane of the request.
    int priority() const { return _priority; }

    /// @return A pointer onto a buffer with a serialized request.
    std::shared_ptr<ProtocolBuffer> const& requestBufferPtr() const { return _requestBufferPtr; }

    /// @return A non-constant reference to buffer for receiving responses from a worker.
    ProtocolBuffer& responseBuffer() { return _responseBuffer; }

    /**
     * Update the completion status of a request to 'success'.
     *
     * This method is supposed to be called upon a successful completion of a request
     * after a valid response is received from a worker and before notifying
     * a subscriber.
     *
     * @param success  The new completion status.
     */
    void setSuccess(bool status) { _success = status; }

    /**
     * Parse the content of the buffer and notify a subscriber
     */
    virtual void parseAndNotify() = 0;

protected:
    /**
     * Construct the object in the default failed (member 'success') state. Hence, there is no
     * need to set this state explicitly unless a request turns out to be
     * a success.
     * @param id  A unique identifier of the request.
     * @param priority  The priority level of a request.
     * @param requestBufferPtr  The input buffer with serialized request.
     * @param responseBufferCapacityBytes  The initial capacity of the response buffer.
     */
    MessageWrapperBase(std::string const& id, int priority,
                       std::shared_ptr<ProtocolBuffer> const& requestBufferPtr,
                       size_t responseBufferCapacityBytes)
            : _success(false),
              _id(id),
              _priority(priority),
              _requestBufferPtr(requestBufferPtr),
              _responseBuffer(responseBufferCapacityBytes) {}

private:
    bool _success;    ///< The completion status to be returned to a subscriber.
    std::string _id;  ///< A unique identifier of the request.
    int _priority;    ///< The priority level of a request.

    /// The buffer with a serialized request.
    std::shared_ptr<ProtocolBuffer> _requestBufferPtr;

    /// The buffer for receiving responses from a worker server.
    ProtocolBuffer _responseBuffer;
};

/**
 * Class template MessageWrapper extends its based to support type-specific
 * treatment (including serialization) of responses from workers.
 */
template <class RESPONSE_TYPE>
class MessageWrapper : public MessageWrapperBase {
public:
    typedef std::function<void(std::string const&, bool, RESPONSE_TYPE const&)> CallbackType;

    MessageWrapper() = delete;
    MessageWrapper(MessageWrapper const&) = delete;
    MessageWrapper& operator=(MessageWrapper const&) = delete;

    ~MessageWrapper() override = default;

    /**
     * The constructor.
     * @param id  A unique identifier of the request.
     * @param priority  The priority level of a request.
     * @param requestBufferPtr  A request serialized into a network buffer.
     * @param responseBufferCapacityBytes  The initial size of the response buffer.
     * @param onFinish  An asynchronous callback function called upon the completion
     *    or failure of the operation.
     */
    MessageWrapper(std::string const& id, int priority,
                   std::shared_ptr<ProtocolBuffer> const& requestBufferPtr,
                   size_t responseBufferCapacityBytes, CallbackType const& onFinish)
            : MessageWrapperBase(id, priority, requestBufferPtr, responseBufferCapacityBytes),
              _onFinish(onFinish) {}

    /// @see MessageWrapperBase::parseResponseAndNotify
    void parseAndNotify() override {
        RESPONSE_TYPE response;
        if (success()) {
            try {
                responseBuffer().parse(response, responseBuffer().size());
            } catch (std::runtime_error const& ex) {
                // The message is corrupt. Google Protobuf will report an error
                // of the following kind:
                // @code
                //   [libprotobuf ERROR google/protobuf/message_lite.cc:123] Can't parse message...
                // @code
                setSuccess(false);
            }
        }
        // Make sure the notification (if requested) is sent just once.
        if (_onFinish != nullptr) {
            _onFinish(id(), success(), response);
            _onFinish = nullptr;
        }
    }

private:
    /// The callback function to be called upon the completion of the request.
    CallbackType _onFinish;
};

/**
 * Class MessengerConnector provides a communication interface for sending/receiving
 * messages to and from worker services. It provides connection multiplexing and
 * automatic reconnects.
 *
 * NOTES ON THREAD SAFETY:
 *
 * - in the implementation of this class a mutex is used to prevent race conditions
 *   when performing internal state transitions.
 *
 * - to avoid deadlocks, only externally called methods of the public API (such
 *   as the ones for sending or cancelling requests) and asynchronous callbacks
 *   are locking the mutex. Those methods are NOT allowed to call each other.
 *   Otherwise deadlocks are imminent.
 *
 * - private methods (where a state transition occurs or which are relying
 *   on specific states) are required to be called with a reference to
 *   the lock acquired prior to the calls.
 */
class MessengerConnector : public std::enable_shared_from_this<MessengerConnector> {
public:
    typedef std::shared_ptr<MessengerConnector> Ptr;

    MessengerConnector() = delete;
    MessengerConnector(MessengerConnector const&) = delete;
    MessengerConnector& operator=(MessengerConnector const&) = delete;

    ~MessengerConnector() = default;

    /**
     * Create a new connector with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  Services of the Replication Framework.
     * @param io_service  The I/O service for communication. The lifespan of
     *   the object must exceed the one of this instance.
     * @param worker  The name of a worker.
     * @return  A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker);

    /**
     * Stop operations.
     */
    void stop();

    /**
     * Initiate sending a message.
     *
     * The response message will be initialized only in case of successful completion
     * of the request. The method may throw exception std::logic_error if
     * the MessangerConnector already has another request registered with the same
     * request 'id'.
     *
     * @param id  A unique identifier of a request.
     * @param priority  The priority level of a request.
     * @param requestBufferPtr  A request serialized into a network buffer.
     * @param onFinish  An asynchronous callback function called upon a completion
     *   or failure of the operation.
     */
    template <class RESPONSE_TYPE>
    void send(std::string const& id, int priority, std::shared_ptr<ProtocolBuffer> const& requestBufferPtr,
              typename MessageWrapper<RESPONSE_TYPE>::CallbackType const& onFinish) {
        _sendImpl(std::make_shared<MessageWrapper<RESPONSE_TYPE>>(id, priority, requestBufferPtr,
                                                                  _bufferCapacityBytes, onFinish));
    }

    /**
     * Cancel an outstanding request.
     *
     * If this call succeeds there won't be any 'onFinish' callback made
     * as provided to the 'onFinish' method in method 'send'.
     *
     * The method may throw std::logic_error if the Messenger doesn't have
     * a request registered with the specified request 'id'.
     *
     * @param id  A unique identifier of a request.
     */
    void cancel(std::string const& id);

    /**
     * Check if a requst is known to the Messenger.
     * @param id  A unique identifier of a request.
     * @return  'true' if the specified request is known to the Messenger.
     */
    bool exists(std::string const& id) const;

private:
    /// @see MessengerConnector::create()
    MessengerConnector(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                       std::string const& worker);

    /**
     * The actual implementation of the operation 'send'.
     * @param ptr  A pointer to the request wrapper object.
     */
    void _sendImpl(MessageWrapperBase::Ptr const& ptr);

    /// State transitions for the connector object
    enum State {
        STATE_INITIAL,       // no communication is happening
        STATE_CONNECTING,    // attempting to connect to a worker service
        STATE_COMMUNICATING  // sending or receiving messages
    };

    /// @return the string representation of the connector's state.
    static std::string _state2string(State state);

    /**
     * Restart the whole operation from scratch.
     *
     * Cancel any asynchronous operation(s) if not in the initial state
     * w/o notifying a subscriber.
     *
     * @note This method is called internally when there is a doubt that
     *   it's possible to do a clean recovery from a failure.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _restart(util::Lock const& lock);

    /**
     * Start resolving the destination worker host & port.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _resolve(util::Lock const& lock);

    /**
     * Callback handler for the asynchronous operation.
     * @param ec  An error code to be checked.
     * @param iter  The host resolver iterator.
     */
    void _resolved(boost::system::error_code const& ec, boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Start resolving the destination worker host & port.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _connect(util::Lock const& lock, boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Callback handler for the asynchronous operation upon its successful
     * completion will trigger a request-specific protocol sequence.
     * @param ec  An error code to be checked.
     * @param iter  The host resolver iterator.
     */
    void _connected(boost::system::error_code const& ec, boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Start a timeout before attempting to restart the connection.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _waitBeforeRestart(util::Lock const& lock);

    /**
     * Callback handler fired for restarting the connection.
     * @param ec  An error code to be checked.
     */
    void _awakenForRestart(boost::system::error_code const& ec);

    /**
     * Lookup for the next available request and begin sending it
     * unless there is another ongoing request at a time of the call.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _sendRequest(util::Lock const& lock);

    /**
     * Callback handler fired upon a completion of the request sending.
     * @param ec  An error code to be checked.
     * @param bytes_transferred  The number of bytes sent.
     */
    void _requestSent(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Begin receiving a response.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     */
    void _receiveResponse(util::Lock const& lock);

    /**
     * Callback handler fired upon a completion of the response receiving.
     * @param ec  An error code to be checked.
     * @param bytes_transferred  The number of bytes sent.
     */
    void _responseReceived(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Synchronously read a protocol frame which carries the length
     * of a subsequent message and return that length along with the completion
     * status of the operation.
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     * @param buf  The buffer to use.
     * @param bytes  The length in bytes extracted from the frame.
     * @return  The completion code of the operation.
     */
    boost::system::error_code _syncReadFrame(util::Lock const& lock, ProtocolBuffer& buf, size_t& bytes);

    /**
     * Synchronously read a response header of a known size. Then parse it
     * and analyze it to ensure its content matches expectations. Return
     * the completion status of the operation.
     *
     * The method will throw exception std::logic_error if the header's
     * content won't match expectations.
     *
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     * @param buf The buffer to use.
     * @param bytes  The expected length of the message (obtained from a preceding
     *   frame) to be received into the network buffer from the network.
     * @param id  A unique identifier of a request to match the 'id' in a response header
     * @return  The completion code of the operation.
     */
    boost::system::error_code _syncReadVerifyHeader(util::Lock const& lock, ProtocolBuffer& buf, size_t bytes,
                                                    std::string const& id);

    /**
     * Synchronously read a message of a known size into the specified buffer.
     * Return the completion status of the operation. After the successful
     * completion of the operation the content of the network buffer can be parsed.
     *
     * @param lock  A lock on MessengerConnector::_mtx must be acquired before
     *   calling this method.
     * @param buf The buffer to use.
     * @param bytes  A expected length of the message (obtained from a preceding frame)
     *   to be received into the network buffer from the network.
     * @return  The completion code of the operation.
     */
    boost::system::error_code _syncReadMessageImpl(util::Lock const& lock, ProtocolBuffer& buf, size_t bytes);

    /**
     * Check if the error status corresponds to a failed operation.
     *
     * @note Normally this method is supposed to be called as the first action
     *   within asynchronous handlers to figure out if an on-going asynchronous
     *   operation was cancelled or failed for some reason. Should this be the case
     *   the caller is supposed to quit right away. It will be up to a code
     *   which initiated the abort to take care of putting the object into
     *   a proper state.
     * @param ec  An error code to be checked.
     * @return 'true' if the operation failed.
     */
    bool _failed(boost::system::error_code const& ec) const;

    /// @return the worker-specific context string
    std::string _context() const;

    // Input parameters.

    ServiceProvider::Ptr const _serviceProvider;

    /// The unique identifier of the worker.
    std::string const _worker;

    /// The cached parameter for the buffer sizes (pulled from
    /// the Configuration upon the construction of the object).
    size_t const _bufferCapacityBytes;

    /// The cached parameter for the interval between reconnection
    /// attempts (pulled from the Configuration upon the construction of
    /// the object).
    unsigned int const _timerIvalSec;

    /// The internal state
    State _state;

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket _socket;
    boost::asio::deadline_timer _timer;

    /// This mutex is meant to avoid race conditions to the internal data
    /// structure between threads that trigger asynchronous I/O events
    /// and threads submitting requests.
    mutable util::Mutex _mtx;

    /// The priority-based queue of requests.
    MessageQueue<MessageWrapperBase> _requests;

    /// The currently processed (being sent) request (if any, otherwise
    /// the pointer is set to nullptr).
    MessageWrapperBase::Ptr _currentRequest;

    /// The intermediate buffer for messages received from a worker.
    ProtocolBuffer _inBuffer;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_MESSENGERCONNECTOR_H
