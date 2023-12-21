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
#ifndef LSST_QSERV_REPLICA_FINDALLREQUEST_H
#define LSST_QSERV_REPLICA_FINDALLREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/requests/RequestMessenger.h"
#include "replica/util/Common.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Messenger;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class FindAllRequest represents known replicas lookup requests within
 * the master controller.
 */
class FindAllRequest : public RequestMessenger {
public:
    typedef std::shared_ptr<FindAllRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    FindAllRequest() = delete;
    FindAllRequest(FindAllRequest const&) = delete;
    FindAllRequest& operator=(FindAllRequest const&) = delete;

    ~FindAllRequest() final = default;

    std::string const& database() const { return _database; }

    bool saveReplicaInfo() const { return _saveReplicaInfo; }

    /// @return target request specific parameters
    FindAllRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @note This operation will return a sensible result only if the operation
     *   finishes with status FINISHED::SUCCESS.
     * @return a reference to a result of the completed request.
     */
    ReplicaInfoCollection const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider a host of services for various communications
     * @param workerName the identifier of a worker node (the one where the chunks
     *   expected to be located)
     * @param database the name of a database
     * @param saveReplicaInfo save replica info in a database
     * @param onFinish an optional callback function to be called upon a completion of
     *   the request
     * @param priority a priority level of the request
     * @param keepTracking keep tracking the request before it finishes or fails
     * @param messenger an interface for communicating with workers
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& workerName, std::string const& database, bool saveReplicaInfo,
                      CallbackType const& onFinish, int priority, bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /// @see Request::startImpl()
    void startImpl(replica::Lock const& lock) final;

    /// @see Request::notify()
    void notify(replica::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(replica::Lock const& lock) final;

    /// @see Request::awaken()
    void awaken(boost::system::error_code const& ec) final;

private:
    /// @see FindAllRequest::create()
    FindAllRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                   std::string const& workerName, std::string const& database, bool saveReplicaInfo,
                   CallbackType const& onFinish, int priority, bool keepTracking,
                   std::shared_ptr<Messenger> const& messenger);

    /**
     * Send the serialized content of the buffer to a worker
     * @param lock a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(replica::Lock const& lock);

    /**
     * Process the completion of the requested operation
     * @param success 'true' indicates a successful response from a worker
     * @param message response from a worker (if success)
     */
    void _analyze(bool success, ProtocolResponseFindAll const& message);

    // Input parameters

    std::string const _database;
    bool const _saveReplicaInfo;
    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    FindAllRequestParams _targetRequestParams;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_FINDALLREQUEST_H
