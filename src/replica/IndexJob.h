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
#ifndef LSST_QSERV_REPLICA_INDEXJOB_H
#define LSST_QSERV_REPLICA_INDEXJOB_H

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

// Qserv headers
#include "replica/Common.h"
#include "replica/Job.h"
#include "replica/IndexRequest.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {
    class Connection;
}}}}}

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure IndexJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct IndexJobResult {
    /// MySQL-specific errors (if any) for chunks are stored in this map
    std::map<std::string,           // worker
             std::map<unsigned int, // chunk
                      std::string>> error;
};

/**
 * Class IndexJob is a class for a family of jobs which broadcast
 * the "secondary index" retrieval requests for the relevant chunks to
 * the workers. Results are either dumped into the specified folder or
 * directly loaded into the "secondary index" of a database.
 */
class IndexJob : public Job  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<IndexJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /// Possible destinations where the harvested data would go
    enum Destination {
        DISCARD,    // do nothing with the data
        FILE,       // write all data into a file
        FOLDER,     // write each chunk's data as a separate file at a folder
        TABLE       // write into the specified or standard "secondary index" table
    };

    /// @return the string representation for a value of the Destination option
    static std::string toString(Destination destination);

    /// @return a value of the enumerator Destination parsed from the input string
    /// @throw invalid_argument if the input value doesn't match any known option of
    ///        the enumerator type
    static Destination fromString(std::string const& str);

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database the name of a database for which the "secondary index"
     *   is built
     * @param hasTransactions  if 'true' then the database's "director" tables
     *   are expected to be partitioned, and the job will extract data (including
     *   column "qserv_trans_id") from a specific MySQL partition.
     * @param transactionId an identifier of a super-transaction which would
     *   limit a scope of the data extraction requests. NOte this request will
     *   be considered only if 'hasTransactions=true'.
     * @param allWorkers engage all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers which are not
     *   in the 'READ-ONLY' state will be involved into the operation.
     * @param destination a destination for the harvested data
     * @param destinationPath depending on a value of the previous parameter
     *   'destination', a value of this parameter could be either either the name
     *   of a file, the name of an existing folder, or the name of a table. For
     *   the FILE destination the empty destination path will trigger dumping
     *   the data onto the Standard Output Stream. For the FOLDER option
     *   the current working directory will be assumed. And for the TABLE option
     *   the empty value would imply the standard "secondary index" table of
     *   the database. A non-empty value for the table would imply the name of
     *   a specific (non-standard) table.
     * @param localFile the flag which is used along with the TABLE destinaton option.
     *   If the flag is set to 'true' then index contribution files retrieved from
     *   workers would be loaded into the destination table using MySQL statement
     *   "LOAD DATA LOCAL INFILE". Otherwise, contributions will be loaded using
     *   "LOAD DATA INFILE", which will require the files be directly visible by
     *   the MySQL server where the table is residing. Note that the non-local
     *   option results in the better performance of the operation. On the other hand,
     *   the local option requires the server be properly configured to allow this
     *   mechanism. The flag is ignored for other destination options.
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId (optional) identifier of a parent job
     * @param onFinish (optional) a function to be called upon a completion of the job
     * @param options (optional) defines the job priority, etc.
     */
    static Ptr create(std::string const& database,
                      bool hasTransactions,
                      TransactionId transactionId,
                      bool allWorkers,
                      Destination destination,
                      std::string const& destinationPath,
                      bool localFile,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    IndexJob() = delete;
    IndexJob(IndexJob const&) = delete;
    IndexJob& operator=(IndexJob const&) = delete;

    /// Non-trivial destructor is needed to abort an ongoing transaction
    /// if needed.
    ~IndexJob() final;

    // Trivial get methods

    std::string const& database()        const { return _database; }
    bool               hasTransactions() const { return _hasTransactions; }
    TransactionId      transactionId()   const { return _transactionId; }
    bool               allWorkers()      const { return _allWorkers; }
    Destination        destination()     const { return _destination; }
    std::string const& destinationPath() const { return _destinationPath; }
    bool               localFile()       const { return _localFile; }

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
    IndexJobResult const& getResultData() const;

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:
    void startImpl(util::Lock const& lock) final;

    void cancelImpl(util::Lock const& lock) final;

    void notify(util::Lock const& lock) final;

private:
    IndexJob(std::string const& database,
             bool hasTransactions,
             TransactionId transactionId,
             bool allWorkers,
             Destination destination,
             std::string const& destinationPath,
             bool localFile,
             Controller::Ptr const& controller,
             std::string const& parentJobId,
             CallbackType const& onFinish,
             Job::Options const& options);

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void _onRequestFinish(IndexRequest::Ptr const& request);

    /**
     * Extract data from the successfully completed requests. The completion
     * state of the request will be evaluated by the method.
     *
     * @param lock on the mutex Job::_mtx to be acquired for protecting
     *   the object's state
     * @param request the request to extract the data to be processed
     */
    void _processRequestData(util::Lock const& lock,
                             IndexRequest::Ptr const& request);

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
    std::list<IndexRequest::Ptr> _launchRequests(util::Lock const& lock,
                                                 std::string const& worker,
                                                 size_t maxRequests=1);

    /**
     * Roll back a database transaction should the one be still open
     * for destination TABLE. The method won't have any effect for other
     * scenarios.
     *
     * @param func  the name of a method/function to report errors
     */
    void _rollbackTransaction(std::string const& func);

private:
    // Input parameters

    std::string   const _database;
    bool          const _hasTransactions;
    TransactionId const _transactionId;
    bool          const _allWorkers;
    Destination   const _destination;
    std::string   const _destinationPath;
    bool          const _localFile;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A collection of chunks to be processed at specific workers
    std::map<std::string, std::queue<unsigned int>> _chunks;

    /// A collection of the in-flight requests (request id is the key) 
    std::map<std::string, IndexRequest::Ptr> _requests;

    /// Database connector is initialized for Destination::TABLE upon arrival
    /// of the very first batch of data. A separate transaction is started 
    /// to load each bunch of data received from workers. The transaction (if
    /// any is still open) is automatically aborted by the destructor or
    /// the request cancellation.
    std::shared_ptr<database::mysql::Connection> _conn;

    /// The result of the operation (gets updated as requests are finishing)
    IndexJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INDEXJOB_H
