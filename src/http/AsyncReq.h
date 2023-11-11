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
#ifndef LSST_QSERV_HTTP_ASYNCREQ_H
#define LSST_QSERV_HTTP_ASYNCREQ_H

// System headers
#include <atomic>
#include <condition_variable>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/beast.hpp"

// Qserv headers
#include "http/Url.h"

// This header declarations
namespace lsst::qserv::http {

/**
 * @brief Class AsyncReq represents a simple asynchronous interface for
 * communicating over the HTTP protocol.
 *
 * The implementation of the class invokes a user-supplied callback (lambda) function
 * upon a completion or a failure of the request.
 *
 * Here is an example of using the class to pull a file and dump its content on
 * to the standard output stream:
 * @code
 *   boost::asio::io_service io_service;
 *   std::shared_ptr<AsyncReq> const reader =
 *       http::AsyncReq::create(
 *           [](auto const& reader) {
 *               if (reader->state() != http::AsyncReq::State::FINISHED) {
 *                   std::cerr << "request failed, state: " << AsyncReq::state2str(reader->state())
 *                        << ", error: " << reader->errorMessage() << std::endl;
 *                   return;
 *               }
 *               cout << reader->responseBody();
 *           },
 *           io_service,
 *           "GET", "http://my.host.domain/data/chunk_0.txt");
 *   });
 *   reader->start();
 *   io_service.run();
 * @code
 *
 * @note Once the start() method gets called w/o throwing an exception, the algorithm
 *   can make repeated attempts to establish a connection to the Web server even if
 *   the server's host and/or port won't be resolved, or if the connection attempts
 *   will be failing, or if attempts to send a request will be failing. A short delay
 *   will be introduced after each unsuccessfull attempt. The request will go into
 *   the FAILED state if 1) there will be a failure in receiving server's response,
 *   2) in case of incorrect data received from the server, or 3) the result is too
 *   large.
 *
 * @note The implementation will open and close a new connection for each request.
 * @note The implementation doesn't support TLS/SSL-based HTTPS protocol.
 */
class AsyncReq : public std::enable_shared_from_this<AsyncReq> {
public:
    /// The function type for notifications on the completion of the operation.
    typedef std::function<void(std::shared_ptr<AsyncReq> const&)> CallbackType;

    enum class State : int {
        CREATED = 0,       ///< The object was created and no request was initiated.
        IN_PROGRESS,       ///< The request is still in progress
        FINISHED,          ///< Final state: the request was deliver to the server and
                           ///  a valid server response was received.
        FAILED,            ///< Final state: failed to deliver the request to the server or
                           ///  receive a valid server response.
        BODY_LIMIT_ERROR,  ///< Final state: the operation failed because the response's body
                           ///  is larger than requested.
        CANCELLED,         ///< Final state: the request was explicitly cancelled before
                           ///  it had a chance to finish.
        EXPIRED            ///< Final state: the request was aborted  before it had
                           ///  a chance to finish due to the timeout expiration.
    };

    /// @return The string representation of the state.
    /// @throw std::invalid_argument For unknown values of the input parameter.
    static std::string state2str(State state);

    AsyncReq() = delete;
    AsyncReq(AsyncReq const&) = delete;
    AsyncReq& operator=(AsyncReq const&) = delete;

    /**
     * @brief Static factory for creating objects of the class.
     *
     * Objects created by the method will be in the state State::CREATED. The actual
     * execution of the request has to be initiated by calling the method start().
     *
     * @param io_service BOOST ASIO service.
     * @param onFinish The callback function to be called upon a completion
     *   (successfull or not) of a request.
     * @param method The case-sensitive name of the HTTP method ('GET', 'POST', 'PUT', 'DELETE').
     * @param url A location of the remote resoure.
     * @param data (Optional) Data to be sent in the request's body.
     * @param headers (Optional) HTTP headers to be send with a request.
     * @param maxResponseBodySize (Optional) The maximum size of the response body.
     *   If a value of the parameter is set to 0 then the default limit of 8M
     *   imposed by the Boost.Beast library will be assumed. If the size of the response
     *   body received from the server will exceed the (explicit or implicit) limit then
     *   the operation will fail and the object will be put into the state State::BODY_LIMIT_ERROR.
     * @param expirationIvalSec (Optional) A timeout to wait before the completion of a request.
     *   The expiration timer starts after the start() method gets called. The timeout
     *   includes all phases of the request's execution, including establishing a connection
     *   to the server, sending the request and waiting for the server's response.
     *   If a value of the parameter is set to 0 then no expiration timeout will be
     *   assumed for the request.
     * @return The shared pointer to the newly created object.
     * @throw std::invalid_argument If empty or invalid values of the input parameters
     *   were provided.
     */
    static std::shared_ptr<AsyncReq> create(boost::asio::io_service& io_service, CallbackType const& onFinish,
                                            std::string const& method, std::string const& url,
                                            std::string const& data = std::string(),
                                            std::unordered_map<std::string, std::string> const& headers =
                                                    std::unordered_map<std::string, std::string>(),
                                            size_t maxResponseBodySize = 0,
                                            unsigned int expirationIvalSec = 0);

    /// Non-trivial destructor is needed to free up allocated resources.
    virtual ~AsyncReq();

    std::string version() const;
    std::string const& method() const { return _method; }
    http::Url const& url() const { return _url; }

    /// @return The current state of the request.
    State state() const { return _state; }

    /**
     * @brief Begin processing a request.
     *
     * Upon successful completion of the method the object's state will be set
     * to State::IN_PROGRESS. Should any problem ocurres with initiating request
     * processing the object's state will be set to State::FAILED and an exception
     * will be thrown.
     *
     * @throw std::logic_error If the operation already started or finished.
     */
    void start();

    /**
     * @brief Cancel the ongoing request.
     * @return 'false' if the request's state was in one of the final states
     *   at a time when the method was called. Return 'true' if the request got
     *   actually cancelled by calling this method.
     */
    bool cancel();

    /// Wait for the completion of the request
    void wait();

    /// @return The last error (message) in case of a failure.
    /// @note The method should be used in one of the final states of the request.
    std::string errorMessage() const;

    /// @return The completion code (HTTP).
    /// @throw std::logic_error If the operation didn't start or it was still in progress.
    /// @note The code is available in states State::FINISHED and State::BODY_LIMIT_ERROR.
    int responseCode() const;

    /// @return The content of response header represented by the key/value map.
    /// @throw std::logic_error If the operation didn't start or it was still in progress.
    /// @note The header is available in states State::FINISHED and State::BODY_LIMIT_ERROR.
    std::unordered_map<std::string, std::string> const& responseHeader() const;

    /// @return The reference to the response's body.
    /// @throw std::logic_error If the operation didn't start or it was still in progress.
    std::string const& responseBody() const;

    /// @return The size (in bytes) of the response's body.
    /// @throw std::logic_error If the operation didn't start or it was still in progress.
    size_t responseBodySize() const;

private:
    /// @see AsyncReq::create()
    AsyncReq(boost::asio::io_service& io_service, CallbackType const& onFinish, std::string const& method,
             std::string const& url, std::string const& data,
             std::unordered_map<std::string, std::string> const& headers, size_t maxResponseBodySize,
             unsigned int expirationIvalSec);

    /**
     * @brief Verify the desired state against the current one.
     *
     * @param lock The lock to be held of _mtx.
     * @param context The calling context.
     * @param desiredStates The desired states (may be more than one) to be verified.
     * @throw std::logic_error If the current state didn't match the desired one.
     */
    void _assertState(std::lock_guard<std::mutex> const& lock, std::string const& context,
                      std::initializer_list<State> const& desiredStates) const;

    // Async operations initiators and handlers.

    void _restart(std::lock_guard<std::mutex> const& lock);
    void _restarted(boost::system::error_code const& ec);
    void _resolve(std::lock_guard<std::mutex> const& lock);
    void _resolved(boost::system::error_code const& ec,
                   boost::asio::ip::tcp::resolver::results_type const& results);
    void _connected(boost::system::error_code const& ec);
    void _sent(boost::system::error_code const& ec, size_t bytesSent);
    void _received(boost::system::error_code const& ec, size_t bytesReceived);
    void _expired(boost::system::error_code const& ec);

    /// Extract the header from the response message and cache it.
    void _extractCacheHeader(std::lock_guard<std::mutex> const& lock);

    /// Log a error along with the request's parameters in the specified context
    void _logError(std::string const& prefix, boost::system::error_code const& ec) const;

    /**
     * @brief Finalize the request.
     * The method will update the operation's state and invoke the user-supplied
     * callback function (if any was provided to the constructor).
     * @param lock The lock to be held of _mtx.
     * @param finalState The final state to be set.
     * @param error (Optional) The error message to be set.
     */
    void _finish(std::lock_guard<std::mutex> const& lock, State finalState,
                 std::string const& error = std::string());

    // Data members.

    boost::asio::io_service& _io_service;
    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket _socket;
    CallbackType _onFinish;
    std::string const _method;
    http::Url const _url;
    std::string const _data;
    std::unordered_map<std::string, std::string> const _headers;
    size_t const _maxResponseBodySize;

    /// This timer is used (if configured) to limit the total run time
    /// of a request. The timer starts when the request is started. And it's
    /// cancelled when a request finishes (successfully or not).
    ///
    /// If the time has a chance to expire then the request would finish
    /// with status: EXPIRED.
    unsigned int const _expirationIvalSec;
    boost::asio::deadline_timer _expirationTimer;

    /// This timer is used to in the communication protocol for requests
    /// which may require multiple retries or any time spacing between network
    /// operation.
    unsigned int const _timerIvalSec = 1;
    boost::asio::deadline_timer _timer;

    /// The current state of the request.
    std::atomic<State> _state{State::CREATED};

    /// A error message from the last failure (if any).
    std::string _error;

    /// The request object is prepared by the method _prepareRequest.
    boost::beast::http::request<boost::beast::http::string_body> _req;

    /// The buffer must persist between reads.
    boost::beast::flat_buffer _buffer;

    /// The response parsers data received from a server. The result is stored in
    /// memory as a string. The maximum size of the response's body is determined
    /// by the parameter 'maxResponseBodySize' passed to the factory method of the class.
    boost::beast::http::response_parser<boost::beast::http::string_body> _res;

    /// Parsed and cached response header
    std::unordered_map<std::string, std::string> _responseHeader;

    /// The mutex for enforcing thread safety of the class public API
    /// and internal operations.
    mutable std::mutex _mtx;

    // Synchronization primitives for implementing AsyncRequest::wait()

    std::atomic<bool> _finished{false};
    std::mutex _onFinishMtx;
    std::condition_variable _onFinishCv;
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_ASYNCREQ_H
