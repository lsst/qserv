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
#ifndef LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H
#define LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/Job.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlResultSet.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class AbortTransactionJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
class AbortTransactionJobResult {
public:
    /// Result sets for the requests for each worker. And result sets
    /// are stored as a list since processing of tables may be
    /// assigned to multiple requests.
    std::map<std::string,std::list<SqlResultSet>> resultSets;

    // The callback type and its convenience types for the work and tables names
    // which are used for exploring results sets of the tables.

    typedef std::string WorkerName;
    typedef std::string TableName;
    typedef std::function<void(WorkerName,TableName,SqlResultSet::ResultSet)> OnTableVisitCallback;

    /**
     * Iterate over the result sets.
     * @param onTableVisit the callback function to be called on each table
     * visited during the iteration.
     */
    void iterate(OnTableVisitCallback const& onTableVisit) const;

    /// @return  JSON representation of the object
    nlohmann::json toJson() const;
};

/**
 * Class AbortTransactionJob represents a tool which will broadcast
 * requests for removing MySQL partitions corresponding to a given
 * super-transaction. The algorithm is designed to work for all types
 * of tables (regular or partitioned) across a select (sub-)set of worker
 * databases. Result sets are collected into structure AbortTransactionJobResult.
 * 
 * An implementation of the job will limit the number of the concurrent
 * in-flight requests to avoid overloading a host where the job issuing
 * Controller runs. The limit will be based on a total number of the requests
 * processing processing threads at the worker services
 * (e.g. N_workers x M_threads_per_worker).
 */
class AbortTransactionJob : public Job  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<AbortTransactionJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param transactionId an identifier of a super-transaction corresponding
     *   to a MySQL partition to be dropped. The transaction must exist, and
     *   it should be in the ABORTED state.
     * @param allWorkers engage all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers which are
     *   not in the 'READ-ONLY' state will be involved into the operation.
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId (optional) identifier of a parent job
     * @param onFinish (optional) callback function to be called upon a completion
     *   of the job
     * @param options (optional) defines the job priority, etc.
     * @return pointer to the created object
     */
    static Ptr create(TransactionId transactionId,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    AbortTransactionJob() = delete;
    AbortTransactionJob(AbortTransactionJob const&) = delete;
    AbortTransactionJob& operator=(AbortTransactionJob const&) = delete;

    ~AbortTransactionJob() final = default;

    // Trivial get methods

    TransactionId transactionId() const { return _transactionId; }

    bool allWorkers() const { return _allWorkers; }

    /**
     * Return the combined result of the operation
     *
     * @note
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when the method
     *   was called
     */
    AbortTransactionJobResult const& getResultData() const;

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    void startImpl(util::Lock const& lock) final;

    void cancelImpl(util::Lock const& lock) final;

    void notify(util::Lock const& lock) final;


private:
    AbortTransactionJob(uint32_t transactionId,
                        bool allWorkers,
                        Controller::Ptr const& controller,
                        std::string const& parentJobId,
                        CallbackType const& onFinish,
                        Job::Options const& options);


    void _onRequestFinish(SqlDeleteTablePartitionRequest::Ptr const& request);
 
    // Input parameters

    TransactionId const _transactionId;
    bool const _allWorkers;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// Set up by the constructor

    TransactionInfo _transactionInfo;
    DatabaseInfo    _databaseInfo;

    std::vector<std::string> _workers;
    
    /// A collection of requests which are either in flight, or finished
    std::list<SqlDeleteTablePartitionRequest::Ptr> _requests;

    // Request counters are used for tracking a condition for
    // completing the job and for computing its final state.

    size_t _numFinished = 0;
    size_t _numSuccess = 0;

    /// The result of the operation (gets updated as requests are finishing)
    AbortTransactionJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H
