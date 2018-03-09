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
#ifndef LSST_QSERV_REPLICA_REQUEST_CONNECTION_H
#define LSST_QSERV_REPLICA_REQUEST_CONNECTION_H

/// RequestConnection.h declares:
///
/// class RequestConnection
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Request.h"
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class RequestConnection is a base class for a family of requests within
  * the replication Controller server.
  */
class RequestConnection
    :   public Request  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestConnection> pointer;

    // Default construction and copy semantics are prohibited

    RequestConnection() = delete;
    RequestConnection(RequestConnection const&) = delete;
    RequestConnection& operator=(RequestConnection const&) = delete;

    /// Destructor
    ~RequestConnection() override = default;

protected:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * NOTE: options 'keepTracking' and 'allowDuplicate' have effect for
     *       specific request only.
     *
     * @param serviceProvider - a provider of various services
     * @param type            - its type name (used informally for debugging)
     * @param worker          - the name of a worker
     * @io_service            - BOOST ASIO service
     * @priority              - may affect an execution order of the request by
     *                          the worker service. Higher number means higher
     *                          priority.
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param allowDuplicate  - follow a previously made request if the current one duplicates it
     */
    RequestConnection(ServiceProvider::pointer const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& type,
                      std::string const& worker,
                      int  priority,
                      bool keepTracking,
                      bool allowDuplicate);

    /**
     * Implement a method defined in the base class.
     */
    void startImpl() override;

    /**
     * Restart the whole operation from scratch.
     *
     * Cancel any asynchronous operation(s) if not in the initial state
     * w/o notifying a subscriber.
     * 
     * NOTE: This method is called internally when there is a doubt that
     * it's possible to do a clean recovery from a failure.
     */
    void restart();

    /// Start resolving the destination worker host & port
    void resolve();

    /// Callback handler for the asynchronious operation
    void resolved(boost::system::error_code const& ec,
                  boost::asio::ip::tcp::resolver::iterator iter);

    /// Start resolving the destination worker host & port
    void connect(boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Callback handler for the asynchronious operation upon its
     * successfull completion will trigger a request-specific
     * protocol sequence.
     */
    void connected(boost::system::error_code const& ec,
                   boost::asio::ip::tcp::resolver::iterator iter);

    /// Start a timeout before attempting to restart the connection
    void waitBeforeRestart();

    /// Callback handler fired for restarting the connection
    void awakenForRestart(boost::system::error_code const& ec);

    /**
     * Implement a method defined in the base class.
     */
    void finishImpl() override;

    /**
      * This method is supposed to be provided by subclasses to begin
      * an actual protocol as required by the subclass.
      */
    virtual void beginProtocol()=0;
    
    /**
     * Synchroniously read a protocol frame which carries the length
     * of a subsequent message and return that length along with the completion
     * status of the operation.
     *
     * @param bytes - the length in bytes extracted from the frame
     *
     * @return the completion code of the operation
     */
    boost::system::error_code syncReadFrame(size_t &bytes);

    /**
     * Synchriniously read a message of a known size. Then parse it
     * given the template parameter of the method. Return the completion status
     * of the operation.
     *
     * @param bytes   - a expected length of the message (obtained from a preceeding frame)
     *                  to be received into the network buffer from the network.
     * @param message - a specific message object to be initialize after reading data from
     *                  the network buffer.
     *
     * @return the completion code of the operation
     */
    template <class MESSAGE_TYPE>
    boost::system::error_code syncReadMessage(size_t const bytes,
                                              MESSAGE_TYPE &message) {

        boost::system::error_code const ec = syncReadMessageImpl(bytes);
        if (not ec) { _bufferPtr->parse(message, bytes); }
        return ec;
    }

    /**
     * Synchriniously read a message of a known size into the network buffer. Return
     * the completion status of the operation. Afyer the successfull completion
     * of the operation the content of the network buffer can be parsed.
     *
     * @param bytes - a expected length of the message (obtained from a preceeding frame)
     *                to be received into the network buffer from the network.
     *
     * @return the completion code of the operation
     */
    boost::system::error_code syncReadMessageImpl(size_t const bytes);

   /**
     * Synchriniously read a response header of a known size. Then parse it
     * and analyze it to ensure its content matches expecations. Return
     * the completion status of the operation.
     *
     * The method will throw exception std::logic_error if the header's
     * content won't match expectations.
     *
     * @param bytes   - a expected length of the message (obtained from a preceeding frame)
     *                  to be received into the network buffer from the network.
     *
     * @return the completion code of the operation
     */
    boost::system::error_code syncReadVerifyHeader(size_t const bytes);
    
protected:

    // Mutable state for network communication

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket   _socket;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_CONNECTION_H