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
#ifndef LSST_QSERV_REPLICA_DIRECTORINDEXREQUEST_H
#define LSST_QSERV_REPLICA_DIRECTORINDEXREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <ostream>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "replica/RequestMessenger.h"

// Forward declarations
namespace lsst::qserv::replica {
class Messenger;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 *  Structure DirectorIndexRequestInfo represents a result of the requests
 */
struct DirectorIndexRequestInfo {
    std::string error;  /// MySQL error (if any)
    std::string data;   /// Index data to be loaded into the "director" index (if success)

    /**
     * Print index data into a file.
     * @param fileName  the name or a file or 'std::cout' if it's empty
     */
    void print(std::string const& fileName = std::string()) const;
};

std::ostream& operator<<(std::ostream& os, DirectorIndexRequestInfo const& info);

/**
 * Class DirectorIndexRequest extracts and returns data to be loaded into
 * the "director" index.
 */
class DirectorIndexRequest : public RequestMessenger {
public:
    typedef std::shared_ptr<DirectorIndexRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    DirectorIndexRequest() = delete;
    DirectorIndexRequest(DirectorIndexRequest const&) = delete;
    DirectorIndexRequest& operator=(DirectorIndexRequest const&) = delete;

    ~DirectorIndexRequest() final = default;

    std::string const& database() const { return _database; }
    std::string const& directorTable() const { return _directorTable; }
    unsigned int chunk() const { return _chunk; }
    bool hasTransactions() const { return _hasTransactions; }
    TransactionId transactionId() const { return _transactionId; }

    /// @return target request specific parameters
    DirectorIndexRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @note the method must be called on requests which are in the FINISHED
     *   state only. Otherwise the resulting structure may be in the undefined state.
     * @note the structure returned by this operation may carry a meaningful
     *   MySQL error code if the worker-side data extraction failed.
     * @return a reference to a result of the completed request
     */
    DirectorIndexRequestInfo const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  a host of services for various communications
     * @param worker  the identifier of a worker node (the one where the chunks
     *   expected to be located)
     * @param database  the name of a database
     * @param directorTable the name of the director table
     * @param chunk  the number of a chunk to be inspected
     * @param hasTransactions  if set to 'true' then the result will also include a column which
     *   stores a value of the corresponding super-transaction
     * @param transactionId  (optional) identifier of a super-transaction. This parameter is used
     *   only if the above defined flag 'hasTransactions' is set.
     * @param onFinish  an optional callback function to be called upon a completion of
     *   the request
     * @param priority  a priority level of the request
     * @param keepTracking  keep tracking the request before it finishes or fails
     * @param messenger  an interface for communicating with workers
     * @return  pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, std::string const& database,
                      std::string const& directorTable, unsigned int chunk, bool hasTransactions,
                      TransactionId transactionId, CallbackType const& onFinish, int priority,
                      bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

    /// @see Request::awaken()
    void awaken(boost::system::error_code const& ec) final;

private:
    DirectorIndexRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                         std::string const& worker, std::string const& database,
                         std::string const& directorTable, unsigned int chunk, bool hasTransactions,
                         TransactionId transactionId, CallbackType const& onFinish, int priority,
                         bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    /**
     * Send the serialized content of the buffer to a worker
     * @param lock a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     * @param success 'true' indicates a successful response from a worker
     * @param message response from a worker (if success)
     */
    void _analyze(bool success, ProtocolResponseDirectorIndex const& message);

    // Input parameters

    std::string const _database;
    std::string const _directorTable;
    unsigned int const _chunk;
    bool const _hasTransactions;
    TransactionId const _transactionId;
    CallbackType _onFinish;

    /// Request-specific parameters of the target request
    DirectorIndexRequestParams _targetRequestParams;

    /// Result of the operation
    DirectorIndexRequestInfo _responseData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DIRECTORINDEXREQUEST_H
