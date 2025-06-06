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
#ifndef LSST_QSERV_REPLICA_DIRECTORINDEXJOB_H
#define LSST_QSERV_REPLICA_DIRECTORINDEXJOB_H

// System headers
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/config/ConfigDatabase.h"
#include "replica/jobs/Job.h"
#include "replica/requests/DirectorIndexRequest.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class ConnectionPool;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DirectorIndexJob is a class for a family of jobs which broadcast
 * the "director" index retrieval requests for the relevant chunks to
 * the workers. Results are directly loaded into the "director" index of
 * the specified director table.
 *
 * Contributions are always loaded into the index table using the "LOCAL" attribute
 * of the query:
 * @code
 * LOAD DATA LOCAL INFILE ...
 * @endcode
 */
class DirectorIndexJob : public Job {
public:
    typedef std::shared_ptr<DirectorIndexJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseName the name of a database for which the "director" index
     *   is built.
     * @param directorTableName the name of the director table
     * @param hasTransactions  if 'true' then the database's "director" tables
     *   are expected to be partitioned, and the job will extract data (including
     *   column "qserv_trans_id") from a specific MySQL partition.
     * @param transactionId an identifier of a super-transaction which would
     *   limit a scope of the data extraction requests. Note this request will
     *   be considered only if 'hasTransactions=true'.
     * @param allWorkers engage all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers which are not
     *   in the 'READ-ONLY' state will be involved into the operation.
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId an identifier of the parent job
     * @param onFinish a function to be called upon a completion of the job
     * @param priority the priority level of the job
     */
    static Ptr create(std::string const& databaseName, std::string const& directorTableName,
                      bool hasTransactions, TransactionId transactionId, bool allWorkers,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    DirectorIndexJob() = delete;
    DirectorIndexJob(DirectorIndexJob const&) = delete;
    DirectorIndexJob& operator=(DirectorIndexJob const&) = delete;

    virtual ~DirectorIndexJob() = default;

    // Trivial get methods

    std::string const& database() const { return _database.name; }
    std::string const& directorTable() const { return _directorTableName; }
    bool hasTransactions() const { return _hasTransactions; }
    TransactionId transactionId() const { return _transactionId; }
    bool allWorkers() const { return _allWorkers; }

    /// @see Job::progress
    virtual Job::Progress progress() const override;

    /**
     * The structure Result represents a combined result received
     * from worker services upon a completion of the job.
     */
    struct Result {
        /// MySQL-specific errors (if any) for chunks are stored in this map
        std::map<std::string,            // worker
                 std::map<unsigned int,  // chunk
                          std::string>>
                error;
        /// @return JSON representation of the object as {<worker>:{<chunk>:<error>}}
        nlohmann::json toJson() const;
    };

    /**
     * Return the combined result of the operation
     *
     * @note The method should be invoked only after the job has
     *  finished (primary status is set to Job::Status::FINISHED). Otherwise
     *  exception std::logic_error will be thrown
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finish at a time when
     *   the method was called
     */
    Result const& getResultData() const;

    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    void startImpl(replica::Lock const& lock) final;
    void cancelImpl(replica::Lock const& lock) final;
    void notify(replica::Lock const& lock) final;

private:
    DirectorIndexJob(std::string const& databaseName, std::string const& directorTableName,
                     bool hasTransactions, TransactionId transactionId, bool allWorkers,
                     Controller::Ptr const& controller, std::string const& parentJobId,
                     CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void _onRequestFinish(DirectorIndexRequest::Ptr const& request);

    /**
     * The method runs by the data loading threads to ingest the "director" index
     * data into the destination table. The method will be pulling requests from
     * the queue _completedRequests and decrement the counter _numLoadingRequests
     * after finishing loading data of each request into the table.
     */
    void _loadDataIntoTable();

    /**
     * Locate anbd return the next request (if any) in the queue _completedRequests.
     * The method gets called by the data loading threads when the threads are ready
     * to process the next request.
     *
     * The request will be removed from the queue. The counter _numLoadingRequests
     * will be incremented.
     *
     * If no requests were found in the queue
     * and while there are still ongoing requests the method will block before any new request will appear in
     * the queue.
     *
     * @note The method will unblock when the jobs will finish
     * and if the job is still in the unfinished while there are ongoing requests in
     * the queue _inFlightRequests the method will block waiting before any request
     * from the latter queue will finish
     *
     * @return DirectorIndexRequest::Ptr A pointer to the next request or nullptr
     *  if the job has laready finished.
     */
    DirectorIndexRequest::Ptr _nextRequest();

    /**
     * Launch a batch of requests with a total number not to exceed the specified
     * limit.
     *
     * @param lock on the mutex Job::_mtx to be acquired for protecting
     *   the object's state
     * @param worker the name of a worker the requests to be sent to
     * @param maxRequests the maximum number of requests to be launched
     * @return a collection of requests launched
     */
    std::list<DirectorIndexRequest::Ptr> _launchRequests(replica::Lock const& lock, std::string const& worker,
                                                         size_t maxRequests = 1);

private:
    // Input parameters

    std::string const _directorTableName;
    bool const _hasTransactions;
    TransactionId const _transactionId;
    bool const _allWorkers;

    CallbackType _onFinish;  ///< Is reset when the job finishes
    DatabaseInfo _database;  ///< Is initialized by the c-tor

    /// A collection of chunks to be processed at specific workers
    std::map<std::string, std::queue<unsigned int>> _chunks;

    /// A collection of the in-flight requests (request id is the key)
    std::map<std::string, DirectorIndexRequest::Ptr> _inFlightRequests;

    /// A collection of the completed requests that have data ready to be loaded
    /// into the "director" table.
    std::list<DirectorIndexRequest::Ptr> _completedRequests;

    /// The number of the on-going operations for ingesting request's data into
    /// the destination trable.
    size_t _numLoadingRequests = 0;

    /// This variable is used for to get the loading threads waiting while
    /// the queue _completedRequests is empty.
    std::condition_variable _cv;

    /// The result of the operation (gets updated as requests are finishing)
    Result _resultData;

    // Job progression counters
    size_t _totalChunks = 0;     ///< The total number of chunks is set when the job is starting.
    size_t _completeChunks = 0;  ///< Is incremented for each processed (regardless of results) chunk.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DIRECTORINDEXJOB_H
