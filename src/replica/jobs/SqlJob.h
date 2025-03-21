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
#ifndef LSST_QSERV_REPLICA_SQLJOB_H
#define LSST_QSERV_REPLICA_SQLJOB_H

// System headers
#include <cstdint>
#include <list>
#include <map>
#include <string>
#include <tuple>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/Controller.h"
#include "replica/jobs/Job.h"
#include "replica/jobs/SqlJobResult.h"
#include "replica/requests/SqlRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlJob is a base class for a family of jobs which broadcast the same
 * query to all worker databases of a setup. Result sets are collected in the above
 * defined data structure.
 */
class SqlJob : public Job {
public:
    typedef std::shared_ptr<SqlJob> Ptr;

    SqlJob() = delete;
    SqlJob(SqlJob const&) = delete;
    SqlJob& operator=(SqlJob const&) = delete;

    ~SqlJob() override = default;

    // Trivial get methods

    uint64_t maxRows() const { return _maxRows; }

    bool allWorkers() const { return _allWorkers; }
    bool ignoreNonPartitioned() const { return _ignoreNonPartitioned; }
    bool ignoreDuplicateKey() const { return _ignoreDuplicateKey; }

    /// @see Job::progress
    virtual Job::Progress progress() const override;

    /**
     * Return the combined result of the operation
     *
     * @note
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown.
     * @return The data structure to be filled upon the completion of the job.
     * @throw std::logic_error If the job didn't finished at a time when
     *   the method was called.
     */
    SqlJobResult const& getResultData() const;

    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

    /**
     * Analyze a result set of a job for a presence of errors and report them if any.
     * The result will be reported as a JSON object. The object will be empty
     * (be evaluated as json::is_null()) if no errors were detected. Otherwise it
     * would be based on the following schema:
     * @code
     *   "job_state":<serialized extended completion status of the job>,
     *   "workers":{
     *     <worker>:{
     *       <table>:{
     *         "status":<serialized error code of a table-specific request>,
     *         "error":<server error string for the request>
     *       }
     *     }
     *   }
     * @code
     * @return A JSON object representing the summary report.
     * @throw std::logic_error If the job didn't finished at a time when
     *   the method was called.
     */
    nlohmann::json getExtendedErrorReport() const;

protected:
    /**
     * @param maxRows An optional limit for the maximum number of rows to be returned
     *   with the request. Leaving the default value of the parameter to 0 will result
     *   in not imposing any explicit restrictions on a size of the result set. Note
     *   that other, resource-defined restrictions will still apply. The later
     *   includes the maximum size of the Google Protobuf objects, the amount of
     *   available memory, etc.
     * @param allWorkers A flag for engaging all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers which are not
     *   in the 'READ-ONLY' sub-state will be involved into the operation.
     * @param controller Is needed launching requests and accessing the Configuration.
     * @param parentJobId An optional identifier of a parent job.
     * @param jobName The name of a job in the persistent state of the Replication system.
     * @param priority The priority level of the job.
     * @param ignoreNonPartitioned The optional flag which if 'true' then don't report as
     *   errors tables for which ProtocolStatusExt::NOT_PARTITIONED_TABLE was reported.
     *   The flag can be useful for tables in which the partitions may have already been removed.
     * @param ignoreDuplicateKey The optional flag which if 'true' then don't report as
     *   errors tables for which ProtocolStatusExt::DUPLICATE_KEY was reported.
     *   The flag can be useful for tables in which the index may already exist.
     */
    SqlJob(uint64_t maxRows, bool allWorkers, Controller::Ptr const& controller,
           std::string const& parentJobId, std::string const& jobName, int priority,
           bool ignoreNonPartitioned = false, bool ignoreDuplicateKey = false);

    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void onRequestFinish(SqlRequest::Ptr const& request);

    /**
     * This method lets a request type-specific subclass to launch requests
     * of the corresponding subtype.
     *
     * @param lock A lock on the mutex Job::_mtx to be acquired for protecting
     *   the object's state.
     * @param workerName The name of a worker the requests to be sent to.
     * @param maxRequestsPerWorker The maximum number of requests to be launched per each worker.
     * @return A collection of requests launched.
     */
    virtual std::list<SqlRequest::Ptr> launchRequests(replica::Lock const& lock,
                                                      std::string const& workerName,
                                                      size_t maxRequestsPerWorker = 1) = 0;

    /**
     * Stop the specified request if it's still running.
     */
    void stopRequest(replica::Lock const& lock, SqlRequest::Ptr const& request) const;

    /**
     * This method lets a request type-specific subclass a chance to process
     * results of the job before transitioning to the finished state.
     *
     * @note The default implementation of the method won't do any processing.
     *   Class-specific implementations may change the extended state if any
     *   problems with the results will be encountered.
     *
     * @param lock  A lock on Job::_mtx must be acquired by a caller of the method.
     * @param extendedState  A specific state to be set upon the completion.
     * @return A collection of requests launched.
     */
    virtual void processResultAndFinish(replica::Lock const& lock, ExtendedState extendedState);

    /**
     * Find out which tables corresponding to the name are expected to exist
     * at the worker as per the Configuration and persistent records for
     * the replicas (for the partitioned tables only). Normally this method
     * is expected to return a single entry for the regular tables, and
     * multiple entries for the partitioned tables (which includes prototype
     * tables if requested, special "overflow" tables, and chunk-specific tables).
     *
     * @param workerName The unique identifier of a worker hosting the tables.
     * @param databaseName The name of the table's database.
     * @param tableName The base name of a table.
     * @param allTables The optional flag (applies to the partitioned tables only)
     *   indicating if all or a subset of tables (as determined by the next
     *   parameter 'overlapTablesOnly') shall be reported. if the flag is set to 'true'
     *   then the next parameter 'overlapTablesOnly' will be ignored.
     * @param overlapTablesOnly The optional flag (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be reported.
     *   If the flag is set to 'true' then overlap tables will be reported.
     *   If the flag is set to 'false' then the chunk tables will be reported.
     *   Note, this parameter is only taken into consideration if the previous
     *   parameter 'allTables' was set to 'false'.
     * @param includeProtoTable The optional flag (applies to the partitioned tables only)
     *   telling the method to include the name of the prototype table into the report.
     *   Note, this parameter is only taken into consideration if a value of
     *   the parameter 'allTables' was set to 'false'.
     * @return A collection of tables found.
     * @throw std::invalid_argument If the database or a table isn't valid.
     */
    std::vector<std::string> workerTables(std::string const& workerName, std::string const& databaseName,
                                          std::string const& tableName, bool allTables = true,
                                          bool overlapTablesOnly = false,
                                          bool includeProtoTable = false) const;

    /**
     * This version of the table locator method searches for tables where actual
     * contributions (successful or not) were attempted in a context of the given
     * transaction. The operation relies upon the persistent records for the transaction
     * contributions.
     *
     * @param workerName The unique identifier of a worker hosting the tables.
     * @param transactionId The unique identifier of the transaction.
     * @param tableName The base name of a table.
     * @param allTables The optional flag (applies to the partitioned tables only)
     *   indicating if all or a subset of tables (as determined by the next
     *   parameter 'overlapTablesOnly') shall be reported. if the flag is set to 'true'
     *   then the next parameter 'overlapTablesOnly' will be ignored.
     * @param overlapTablesOnly The optional flag (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be reported.
     *   If the flag is set to 'true' then overlap tables will be reported.
     *   If the flag is set to 'false' then the chunk tables will be reported.
     *   Note, this parameter is only taken into consideration if the previous
     *   parameter 'allTables' was set to 'false'.
     * @param includeProtoTable The optional flag (applies to the partitioned tables only)
     *   telling the method to include the name of the prototype table into the report.
     *   Note, this parameter is only taken into consideration if a value of
     *   the parameter 'allTables' was set to 'false'.
     * @return A collection of tables found.
     * @throw std::invalid_argument If the database or a table isn't valid.
     */
    std::vector<std::string> workerTables(std::string const& workerName, TransactionId const& transactionId,
                                          std::string const& tableName, bool allTables = true,
                                          bool overlapTablesOnly = false,
                                          bool includeProtoTable = false) const;

    /**
     * The algorithm will distribute tables between the specified number of
     * bins. The resulting collection will be empty if the input collection
     * of tables is empty or if the number of bins is 0, and the result will
     * not have empty bins.
     *
     * @param allTables All known tables.
     * @param numBins The total number of bins for distributing tables,
     * @return Tables distributed between the bins.
     */
    static std::vector<std::vector<std::string>> distributeTables(std::vector<std::string> const& allTables,
                                                                  size_t numBins);

    /**
     * @brief Get a copy of the result data object in its current state, even if it's
     *   not complete.
     * @param lock A lock on Job::_mtx must be acquired by a caller of the method.
     * @return SqlJobResult The current state of the result.
     */
    SqlJobResult getResultData(replica::Lock const& lock) const { return _resultData; }

private:
    /**
     * Verify if the database and the table are known to the Configuration,
     * and obtain the partitioning status of the table.
     *
     * @return A value of 'true' if this is the partitioned table.
     * @throw std::invalid_argument If the database or a table isn't valid.
     */
    bool _isPartitioned(std::string const& databaseName, std::string const& tableName) const;

    // Input parameters
    uint64_t const _maxRows;
    bool const _allWorkers;
    bool const _ignoreNonPartitioned;
    bool const _ignoreDuplicateKey;

    /// A collection of requests implementing the operation
    std::vector<SqlRequest::Ptr> _requests;

    /// This counter is used for tracking a condition for completing the job
    /// before computing its final state.
    size_t _numFinished = 0;

    /// The result of the operation (gets updated as requests are finishing)
    SqlJobResult _resultData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLJOB_H
