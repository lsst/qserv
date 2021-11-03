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
#ifndef LSST_QSERV_REPLICA_DISPOSEREQUEST_H
#define LSST_QSERV_REPLICA_DISPOSEREQUEST_H

// System headers
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "replica/RequestMessenger.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Messenger;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DisposeRequestResult represents a result of the operation
 * reported by a worker service. The content of the structure is set
 * for all identifiers passed into a request upon a successful completion
 * of the request.
 */
class DisposeRequestResult {
public:
    DisposeRequestResult() = default;
    DisposeRequestResult(DisposeRequestResult const&) = default;
    DisposeRequestResult& operator=(DisposeRequestResult const&) = default;

    /// The normal constructor for initializing data members from the protocol object
    explicit DisposeRequestResult(ProtocolResponseDispose const& message);

    /// Completion status for an identifier
    struct Status {
        std::string id;
        bool disposed = false;
    };
    std::vector<Status> ids;
};


/**
 * Class DisposeRequest represents Controller-side requests for "garbage
 * collecting" requests at workers.
 *
 * @note Requests of this type don't have any persistent states. 
 */
class DisposeRequest: public RequestMessenger  {
public:
    typedef std::shared_ptr<DisposeRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    DisposeRequest() = delete;
    DisposeRequest(DisposeRequest const&) = delete;
    DisposeRequest& operator=(DisposeRequest const&) = delete;

    ~DisposeRequest() final = default;

    std::vector<std::string> const& targetIds() const { return _targetIds; }

    /**
     * @note This operation will return a sensible result only if the operation
     *   finishes with status FINISHED::SUCCESS
     * @return a reference to a result obtained from a remote service
     */
    DisposeRequestResult const& responseData() const;

    /// Extend the base class implementation by adding results of the operation
    /// to the output.
    /// @see Request::toString
    std::string toString(bool extended = false) const final;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider provider of various services
     * @param worker identifier of a worker node
     * @param targetIds a collection unique identifiers of requests to be disposed
     * @param onFinish (optional) callback function to call upon completion
     *   of the request
     * @param priority priority level of the request
     * @param keepTracking keep tracking the request before it finishes or fails
     * @param messenger interface for communicating with workers
     *
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::vector<std::string> const& targetIds,
                      CallbackType const& onFinish,
                      int  priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:
    /// @see Request::startImpl
    void startImpl(util::Lock const& lock) final;

    /// @see Request::notify
    void notify(util::Lock const& lock) final;

    /// @note No persistent state for this type of requests
    /// @see Request::savePersistentState
    void savePersistentState(util::Lock const& lock) final {}

private:
    DisposeRequest(ServiceProvider::Ptr const& serviceProvider,
                   boost::asio::io_service& io_service,
                   std::string const& worker,
                   std::vector<std::string> const& targetIds,
                   CallbackType const& onFinish,
                   int  priority,
                   bool keepTracking,
                   std::shared_ptr<Messenger> const& messenger);

    /**
     * Send the serialized content of the buffer to a worker
     * @param lock a lock on Request::_mtx must be acquired before calling
     *   this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     * @param success 'true' indicates a successful response from a worker
     * @param message response from a worker (if success)
     */
    void _analyze(bool success, ProtocolResponseDispose const& message);

    // Input parameters

    std::vector<std::string> const _targetIds;

    CallbackType _onFinish; ///< @note is reset when the request finishes

    /// The transient representation of the data received from a worker
    /// upon a successful completion of a request.
    DisposeRequestResult _responseData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DISPOSEREQUEST_H
