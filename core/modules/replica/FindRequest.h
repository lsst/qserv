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
#ifndef LSST_QSERV_REPLICA_FIND_REQUEST_H
#define LSST_QSERV_REPLICA_FIND_REQUEST_H

/// FindRequest.h declares:
///
/// Common classes shared by all implementations:
///
///   class FindRequest
///
/// Request implementations based on multiplexed connectors provided by
/// base class RequestMessenger:
///
///   class FindRequestM
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

/**
  * Class FindRequestM represents a transient state of the replica lookup
  * requests within the master controller for deleting replicas.
  */
class FindRequestM
    :   public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FindRequestM> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    FindRequestM() = delete;
    FindRequestM(FindRequestM const&) = delete;
    FindRequestM& operator=(FindRequestM const&) = delete;

    /// Destructor
    ~FindRequestM() final = default;

    // Trivial acccessors

    std::string const& database() const        { return _database; }
    unsigned int       chunk() const           { return _chunk; }
    bool               computeCheckSum() const { return _computeCheckSum; }

    /// Return target request specific parameters
    FindRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * Return a refernce to a result obtained from a remote service.
     *
     * Note that this operation will return a sensible result only if the operation
     * finishes with status FINISHED::SUCCESS
     */
    ReplicaInfo const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one where the chunk is
     *                           expected to be located) at a destination of the chunk
     * @param database         - the name of a database
     * @param chunk            - the number of a chunk to find (implies all relevant tables)
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     * @param priority         - a priority level of the request
     * @param computeCheckSum  - tell a worker server to compute check/control sum on each file
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param messenger        - an interface for communicating with workers
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      unsigned int  chunk,
                      CallbackType onFinish,
                      int  priority,
                      bool computeCheckSum,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

private:

    /**
     * Construct the request with the pointer to the services provider.
     */
    FindRequestM(ServiceProvider::Ptr const& serviceProvider,
                 boost::asio::io_service& io_service,
                 std::string const& worker,
                 std::string const& database,
                 unsigned int  chunk,
                 CallbackType onFinish,
                 int  priority,
                 bool computeCheckSum,
                 bool keepTracking,
                 std::shared_ptr<Messenger> const& messenger);

    /**
      * Implement the method declared in the base class
      *
      * @see Request::startImpl()
      */
    void startImpl() final;

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait();

    /// Callback handler for the asynchronious operation
    void awaken(boost::system::error_code const& ec);

    /// Send the serialized content of the buffer to a worker
    void send();

    /// Process the completion of the requested operation
    void analyze(bool success,
                lsst::qserv::proto::ReplicationResponseFind const& message);

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void notify() final;

private:

    // Parameters of the object

    std::string  _database;
    unsigned int _chunk;
    bool         _computeCheckSum;

    /// Registered callback to be called when the operation finishes
    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    FindRequestParams _targetRequestParams;

    /// The results reported by a worker service
    ReplicaInfo _replicaInfo;
};

typedef FindRequestM FindRequest;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FIND_REQUEST_H
