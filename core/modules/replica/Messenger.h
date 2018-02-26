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
#ifndef LSST_QSERV_REPLICA_MESSENGER_H
#define LSST_QSERV_REPLICA_MESSENGER_H

/// Messenger.h declares:
///
/// class Messenger
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <map>
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/MessengerConnector.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class ProtocolBuffer;
class ServiceProvider;

/**
 * This class provides a communication interface for sending/receiving messages
 * to and from worker services. It provides connection multiplexing and automatic
 * reconnects.
 */
class Messenger
    :   public std::enable_shared_from_this<Messenger> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Messenger> pointer;

    // Default construction and copy semantics are prohibited

    Messenger () = delete;
    Messenger (Messenger const&) = delete;
    Messenger& operator= (Messenger const&) = delete;

    /// Destructor
    virtual ~Messenger ();

    /**
     * Create a new messenger with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param io_service       - the I/O service for communication. The lifespan of
     *                           the object must exceed the one of this instance
     *                           of the Messenger.
     */
    static pointer create (ServiceProvider&         serviceProvider,
                           boost::asio::io_service& io_service);

    /**
     * Stop operations
     */
    void stop ();

    /**
     * Initiate sending a message
     *
     * The response message will be initialized only in case of successfull completion
     * of the transaction. The method may throw exception std::invalid_argument if
     * the worker name is not valid, and std::logic_error if the Messanger already
     * has another transaction registered with the same transaction 'id'.
     * 
     * @param worker            - the name of a worker
     * @param id                - a unique identifier of a request
     * @param requestBufferPtr  - a request serielized into a network buffer
     * @param onFinish          - an asynchronious callback function called upon a completion
     *                            or failure of the operation
     */
    template <class RESPONSE_TYPE>
    void send (std::string  const& worker,
               std::string  const& id,
               std::shared_ptr<replica::ProtocolBuffer> const& requestBufferPtr,
               std::function<void(std::string const&,
                                  bool,
                                  RESPONSE_TYPE const&)> onFinish) {

        // Forward the request to the corresponidng worker
        connector(worker)->send<RESPONSE_TYPE> (id,
                                                requestBufferPtr,
                                                onFinish);
    }

    /**
     * Cancel an outstanding transaction
     *
     * If this call succeeds there won't be any 'onFinish' callback made
     * as provided to the 'onFinish' method in method 'send'.
     *
     * For unknown worker names exception std::invalid_argument will be
     * thrown. The method may also throw std::logic_error if the Messanger
     * doesn't have a transaction registered with the specified transaction 'id'.
     *
     * @param worker - the name of a worker
     * @param id     - a unique identifier of a request
     *
     * @return the completion status of the operation
     */
    void cancel (std::string const& worker,
                 std::string const& id);

    /**
     * Return 'true' if the specified requst is known to the Messenger
     *
     * @param worker - the name of a worker
     * @param id     - a unique identifier of a request
     */
    bool exists (std::string const& worker,
                 std::string const& id) const;

private:

    /**
     * The constructor
     */
    Messenger (ServiceProvider         &serviceProvider,
               boost::asio::io_service &io_service);

    /**
     * Locate and return a connector for the specified worker. Throw exception
     * std::invalid_argument if the worker is unknown.
     *
     * @param - the name of a worker
     *
     * @return a pointer to the connector
     */
    MessengerConnector::pointer const& connector (std::string const& worker) const;

private:

    /// Connection providers for individual workers
    std::map<std::string, MessengerConnector::pointer> _connector;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_MESSENGER_H