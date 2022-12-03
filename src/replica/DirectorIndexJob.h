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
#include "replica/Common.h"
#include "replica/Job.h"
#include "replica/DirectorIndexRequest.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class Connection;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DirectorIndexJob is a class for a family of jobs which broadcast
 * the "director" index retrieval requests for the relevant chunks to
 * the workers. Results are directly loaded into the "director" index of
 * the specified director table.
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
     * @param localFile If the flag is set to 'true' then index contribution files
     *   retrieved from workers would be loaded into the "director" index" table using MySQL
     *   statement "LOAD DATA LOCAL INFILE". Otherwise, contributions will be loaded
     *   using "LOAD DATA INFILE", which will require the files be directly visible by
     *   the MySQL server where the table is residing. Note that the non-local
     *   option results in the better performance of the operation. On the other hand,
     *   the local option requires the server be properly configured to allow this
     *   mechanism.
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId an identifier of the parent job
     * @param onFinish a function to be called upon a completion of the job
     * @param priority the priority level of the job
     */
    static Ptr create(std::string const& databaseName, std::string const& directorTableName,
                      bool hasTransactions, TransactionId transactionId, bool allWorkers, bool localFile,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    DirectorIndexJob() = delete;
    DirectorIndexJob(DirectorIndexJob const&) = delete;
    DirectorIndexJob& operator=(DirectorIndexJob const&) = delete;

    /// Non-trivial destructor is needed to abort an ongoing transaction
    /// if needed.
    ~DirectorIndexJob() final;

    // Trivial get methods

    std::string const& database() const { return _database.name; }
    std::string const& directorTable() const { return _directorTableName; }
    bool hasTransactions() const { return _hasTransactions; }
    TransactionId transactionId() const { return _transactionId; }
    bool allWorkers() const { return _allWorkers; }
    bool localFile() const { return _localFile; }

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
    void startImpl(util::Lock const& lock) final;
    void cancelImpl(util::Lock const& lock) final;
    void notify(util::Lock const& lock) final;

private:
    DirectorIndexJob(std::string const& databaseName, std::string const& directorTableName,
                     bool hasTransactions, TransactionId transactionId, bool allWorkers, bool localFile,
                     Controller::Ptr const& controller, std::string const& parentJobId,
                     CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void _onRequestFinish(DirectorIndexRequest::Ptr const& request);

    /**
     * Extract data from the successfully completed requests. The completion
     * state of the request will be evaluated by the method.
     *
     * @param lock on the mutex Job::_mtx to be acquired for protecting
     *   the object's state
     * @param request the request to extract the data to be processed
     */
    void _processRequestData(util::Lock const& lock, DirectorIndexRequest::Ptr const& request);

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
    std::list<DirectorIndexRequest::Ptr> _launchRequests(util::Lock const& lock, std::string const& worker,
                                                         size_t maxRequests = 1);

    /**
     * Roll back a database transaction should the one be still open.
     * @param func  the name of a method/function to report errors
     */
    void _rollbackTransaction(std::string const& func);

private:
    // Input parameters

    std::string const _directorTableName;
    bool const _hasTransactions;
    TransactionId const _transactionId;
    bool const _allWorkers;
    bool const _localFile;

    CallbackType _onFinish;  /// @note is reset when the job finishes

    DatabaseInfo _database;  /// Initialized by the c-tor

    /// A collection of chunks to be processed at specific workers
    std::map<std::string, std::queue<unsigned int>> _chunks;

    /// A collection of the in-flight requests (request id is the key)
    std::map<std::string, DirectorIndexRequest::Ptr> _requests;

    /// Database connector is initialized upon arrival
    /// of the very first batch of data. A separate transaction is started
    /// to load each bunch of data received from workers. The transaction (if
    /// any is still open) is automatically aborted by the destructor or
    /// the request cancellation.
    std::shared_ptr<database::mysql::Connection> _conn;

    /// The result of the operation (gets updated as requests are finishing)
    Result _resultData;

    // Job progression counters
    size_t _totalChunks = 0;     ///< The total number of chunks is set when the job is starting.
    size_t _completeChunks = 0;  ///< Is incremented for each processed (regardless of results) chunk.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DIRECTORINDEXJOB_H
