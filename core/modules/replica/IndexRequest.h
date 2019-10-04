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
#ifndef LSST_QSERV_REPLICA_INDEXREQUEST_H
#define LSST_QSERV_REPLICA_INDEXREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

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
 *  Structure IndexInfo represents a result of the requests
 */
struct IndexInfo {
    std::string error;  /// MySQL error (if any)
    std::string data;   /// Index data to be loaded into the "secondary index" (if success)
};

/**
 * Class IndexRequest extracts and returns data to be loaded into
 * the "secondary index"
 */
class IndexRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<IndexRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    IndexRequest() = delete;
    IndexRequest(IndexRequest const&) = delete;
    IndexRequest& operator=(IndexRequest const&) = delete;

    ~IndexRequest() final = default;

    // Trivial get methods

    std::string const& database()        const { return _database; }
    unsigned int       chunk()           const { return _chunk; }
    bool               hasTransactions() const { return _hasTransactions; }
    unsigned int       transactionId()   const { return _transactionId; }


    /// Return target request specific parameters
    IndexRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return a reference to a result of the completed request
     *
     * @note the method must be called on requests which are in the FINISHED
     * state only. Otherwise the resulting structure may be in the undefined state.
     *
     * @note the structure returned by this operation may carry a meaningful
     * MySQL error code if the worker-side data extraction failed.
     */
    IndexInfo const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   a host of services for various communications
     *
     * @param worker
     *   the identifier of a worker node (the one where the chunks
     *   expected to be located)
     *
     * @param database
     *   the name of a database
     *
     * @param chunk
     *   the number of a chunk to be inspected
     *
     * @param hasTransactions
     *   if set to 'true' then the result will also include a column which
     *   stores a value of the corresponding super-transaction
     *
     * @param transactionId
     *   (optional) identifier of a super-transaction. This parameter is used
     *   only if the above defined flag 'hasTransactions' is set.
     *
     * @param onFinish
     *   an optional callback function to be called upon a completion of
     *   the request
     * 
     * @param priority
     *   a priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   an interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      unsigned int chunk,
                      bool hasTransactions,
                      unsigned int transactionId,
                      CallbackType const& onFinish,
                      int  priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);


    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

private:

    /// @see IndexRequest::create()
    IndexRequest(ServiceProvider::Ptr const& serviceProvider,
                 boost::asio::io_service& io_service,
                 std::string const& worker,
                 std::string const& database,
                 unsigned int chunk,
                 bool hasTransactions,
                 unsigned int transactionId,
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
                  ProtocolResponseIndex const& message);


    // Input parameters

    std::string  const _database;
    unsigned int const _chunk;
    bool         const _hasTransactions;
    unsigned int const _transactionId;
    CallbackType       _onFinish;

    /// Request-specific parameters of the target request
    IndexRequestParams _targetRequestParams;

    /// Result of the operation
    IndexInfo _indexInfo;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INDEXREQUEST_H
