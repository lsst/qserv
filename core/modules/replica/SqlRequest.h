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
#ifndef LSST_QSERV_REPLICA_SQLREQUEST_H
#define LSST_QSERV_REPLICA_SQLREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/RequestMessenger.h"
#include "replica/SqlResultSet.h"
#include "replica/protocol.pb.h"

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
 * Class SqlRequest represents Controller-side requests for initiating
 * database queries at a remote worker nodes. The requests are sent over
 * the controller-worker protocol and they are executed by the the worker-side
 * framework.
 *
 * In case of a successful completion of a request an object of this request class
 * will receive a result set (if any) of the query.
 */
class SqlRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlRequest() = delete;
    SqlRequest(SqlRequest const&) = delete;
    SqlRequest& operator=(SqlRequest const&) = delete;

    ~SqlRequest() final = default;

    // Trivial get methods

    std::string const& query()    const { return _query; }
    std::string const& user()     const { return _user; }
    std::string const& password() const { return _password; }

    uint64_t maxRows() const { return _maxRows; }

    /// @return target request specific parameters
    SqlRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return
     *   a reference to a result obtained from a remote service.
     *
     * @note
     *   This operation will return a sensible result only if the operation
     *   finishes with status FINISHED::SUCCESS
     */
    /**
     * @return
     *   request-specific extended data reported upon a successful
     *   completion of the request
     */
    SqlResultSet const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param query
     *   the query to be executed
     *
     * @param user
     *   the name of a database account for connecting to the database service
     *
     * @param password
     *   a database for connecting to the database service
     *
     * @param maxRows
     *   (optional) limit for the maximum number of rows to be returned with the request.
     *   Laving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. NOte that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password,
                      uint64_t maxRows,
                      CallbackType const& onFinish,
                      int  priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

protected:

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

private:

    /// @see SqlRequest::create()
    SqlRequest(ServiceProvider::Ptr const& serviceProvider,
               boost::asio::io_service& io_service,
               std::string const& worker,
               std::string const& query,
               std::string const& user,
               std::string const& password,
               uint64_t maxRows,
               CallbackType const& onFinish,
               int  priority,
               bool keepTracking,
               std::shared_ptr<Messenger> const& messenger);

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /**
     * Callback handler for the asynchronous operation
     *
     * @param ec
     *   error code to be checked
     */
    void _awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     *
     * @param success
     *   'true' indicates a successful response from a worker
     *
     * @param message
     *   response from a worker (if success)
     */
    void _analyze(bool success,
                  ProtocolResponseSql const& message);

    // Input parameters

    std::string const _query;
    std::string const _user;
    std::string const _password;
    uint64_t    const _maxRows;
    CallbackType      _onFinish;    /// @note is reset when the request finishes

    /// Request-specific parameters of the target request
    SqlRequestParams _targetRequestParams;

    /// The results reported by a worker service
    SqlResultSet _responseData;

    /**
     * This request implements an adaptive request tracking algorithm
     * for following request status on worker nodes. Once the first message
     * is sent to a worker the request tracking timer is launched with
     * with the initial value of the interval (stored in the data
     * member SqlRequest::_currentTimeIvalMsec).
     * Each subsequent activation of the timer is made with an interval which is
     * twice as long as the previous one until a limit set in the base class
     * member is reached:
     *
     * @see Request::timerIvalSec()
     *
     * After that the above mentioned (fixed) interval will always be used
     * untill the request finishes or fails (or gets cancelled, expires, etc.)
     *
     * This algorithm addresses three problems:
     * - it allows nearly real-time response for quick requests
     * - it prevents flooding in the network
     * - it doesn't cause an excessive use of resources on either ends of
     *   the Replication system
     *
     * @return
     *   the next value of the delay expressed in milliseconds
     */
    unsigned int nextTimeIvalMsec();

    /// @see SqlRequest::nextTimeIvalMsec()
    unsigned int _currentTimeIvalMsec = 10;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLREQUEST_H
