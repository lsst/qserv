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
#ifndef LSST_QSERV_REPLICA_MESSENGER_H
#define LSST_QSERV_REPLICA_MESSENGER_H

// System headers
#include <functional>
#include <map>
#include <memory>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/MessengerConnector.h"
#include "replica/protocol.pb.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst { namespace qserv { namespace replica {
class ProtocolBuffer;
}}}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class Messenger provides a communication interface for sending/receiving messages
 * to and from worker services. It provides connection multiplexing and automatic
 * reconnects.
 */
class Messenger : public std::enable_shared_from_this<Messenger> {
public:
    typedef std::shared_ptr<Messenger> Ptr;

    Messenger() = delete;
    Messenger(Messenger const&) = delete;
    Messenger& operator=(Messenger const&) = delete;

    ~Messenger() = default;

    /**
     * Create a new messenger with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  Services of the Replication Framework.
     * @param io_service  The I/O service for communication. The lifespan of
     *   the object must exceed the one of this instance
     *   of the Messenger.
     * @return  A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service);

    /**
     * Stop operations
     */
    void stop();

    /**
     * Initiate sending a message
     *
     * The response message will be initialized only in case of successful completion
     * of the request. The method may throw exception std::invalid_argument if
     * the worker name is not valid, and std::logic_error if the Messenger already
     * has another request registered with the same request 'id'.
     *
     * @param worker  The name of a worker.
     * @param id  A unique identifier of a request.
     * @param priority  The priority level of a request.
     * @param requestBufferPtr  A request serialized into a network buffer.
     * @param onFinish  An asynchronous callback function called upon a completion
     *   or failure of the operation
     */
    template <class RESPONSE_TYPE>
    void send(std::string const& worker, std::string const& id, int priority,
              std::shared_ptr<ProtocolBuffer> const& requestBufferPtr,
              std::function<void(std::string const&, bool, RESPONSE_TYPE const&)> onFinish) {
        _connector(worker)->send<RESPONSE_TYPE>(id, priority, requestBufferPtr, onFinish);
    }

    /**
     * Cancel an outstanding request
     *
     * If this call succeeds there won't be any 'onFinish' callback made
     * as provided to the 'onFinish' method in method 'send'.
     *
     * For unknown worker names exception std::invalid_argument will be
     * thrown. The method may also throw std::logic_error if the Messenger
     * doesn't have a request registered with the specified request 'id'.
     *
     * @param worker  The name of a worker.
     * @param id  A unique identifier of a request.
     * @return  The completion status of the operation.
     */
    void cancel(std::string const& worker, std::string const& id);

    /**
     * Return 'true' if the specified request is known to the Messenger
     *
     * @param worker The name of a worker.
     * @param id  A unique identifier of a request.
     */
    bool exists(std::string const& worker, std::string const& id);

private:
    /// @see Messenger::create()
    Messenger(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service);

    /**
     * Locate and return a connector for the specified worker
     * @param  The name of a worker.
     * @return  A pointer to the connector.
     * @throw std::invalid_argument  If the worker is unknown.
     */
    MessengerConnector::Ptr const& _connector(std::string const& worker);

    /// @return The context string for the given worker (used for reporting errors and logging).
    static std::string _context(std::string const& worker);

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    boost::asio::io_service& _io_service;

    /// The mutex for implementing the synchronized management of the connections.
    mutable util::Mutex _mtx;

    /// Connection providers for individual workers
    std::map<std::string, MessengerConnector::Ptr> _workerConnector;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_MESSENGER_H
