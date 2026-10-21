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
#ifndef LSST_QSERV_REPLICA_HTTPDIRECTORINDEXREQUEST_H
#define LSST_QSERV_REPLICA_HTTPDIRECTORINDEXREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/requests/http/HttpRequest.h"
#include "replica/util/Common.h"
#include "replica/util/DirectorIndexRequestInfo.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpDirectorIndexRequest extracts and returns data to be loaded into
 * the "director" index.
 */
class HttpDirectorIndexRequest : public HttpRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpDirectorIndexRequest>)> CallbackType;

    /**
     * Create a new request with specified parameters.
     *
     * Class-specific parameters are documented below:
     * @param database The name of a database.
     * @param directorTable The name of the director table.
     * @param chunk The number of a chunk to be inspected.
     * @param hasTransactions If set to 'true' then the result will also include a column which
     *   stores a value of the corresponding super-transaction.
     * @param transactionId The (optional) identifier of a super-transaction. This parameter is used
     *   only if the above defined flag 'hasTransactions' is set.
     * @see The very base class for the description of the common parameters of all subclasses.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpDirectorIndexRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& database, std::string const& directorTable, unsigned int chunk,
            bool hasTransactions, TransactionId transactionId, CallbackType const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    HttpDirectorIndexRequest() = delete;
    HttpDirectorIndexRequest(HttpDirectorIndexRequest const&) = delete;
    HttpDirectorIndexRequest& operator=(HttpDirectorIndexRequest const&) = delete;

    /// Non-trivial destructor is needed to delete the data file that is created
    /// upon successful completion of the request.
    virtual ~HttpDirectorIndexRequest() final;

    std::string const& database() const { return _database; }
    std::string const& directorTable() const { return _directorTable; }
    unsigned int chunk() const { return _chunk; }
    bool hasTransactions() const { return _hasTransactions; }
    TransactionId transactionId() const { return _transactionId; }

    /**
     * @note the method must be called on requests which are in the FINISHED
     *   state only. Otherwise the resulting structure may be in the undefined state.
     * @note the structure returned by this operation may carry a meaningful
     *   MySQL error code if the worker-side data extraction failed.
     * @return a reference to a result of the completed request
     */
    DirectorIndexRequestInfo const& responseData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;
    virtual void savePersistentState(replica::Lock const& lock) final;
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) final;

private:
    HttpDirectorIndexRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                             std::string const& database, std::string const& directorTable,
                             unsigned int chunk, bool hasTransactions, TransactionId transactionId,
                             CallbackType const& onFinish, int priority, bool keepTracking);

    /**
     * Analyze the HTTP response and write the data into the file. Finalize the request upon completion.
     * @note The method will fail the request should any problems were encountered while creating
     *  the file or writing data into the file. The method will also tell the worker service
     *   to delete the remote file.
     * @param lock A lock on the mutex mtx acquired before calling the method.
     * @param req The HTTP request object containing the response data.
     * @param error An error message if any error occurred during the HTTP request.
     */
    void _writeDataIntoFileAndFinish(replica::Lock const& lock, std::shared_ptr<http::AsyncReq> req,
                                     std::string error);

    // Input parameters

    std::string const _database;
    std::string const _directorTable;
    unsigned int const _chunk;
    bool const _hasTransactions;
    TransactionId const _transactionId;
    CallbackType _onFinish;

    /// Flag indicating whether the request is currently reading data from the remote file.
    /// The flag is set in _analyze() before submitting a request to retrieve data from the file.
    bool _readingData = false;

    /// Result of the operation
    DirectorIndexRequestInfo _responseData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPDIRECTORINDEXREQUEST_H
