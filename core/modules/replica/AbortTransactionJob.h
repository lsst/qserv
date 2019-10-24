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
    
    /// Completion status of an operation over tables
    std::map<std::string,           // worker
             std::map<std::string,  // table
                      bool>> completed;

    /// Result sets for the requests
    std::map<std::string,           // worker
             std::map<std::string,  // table
                      SqlResultSet>> resultSets;

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
     * @param transactionId
     *   an identifier of a super-transaction corresponding to a MySQL partition
     *   to be dropped. The transaction must exist, and it should be in
     *   the ABORTED state.
     *
     * @param allWorkers
     *   engage all known workers regardless of their status. If the flag
     *   is set to 'false' then only 'ENABLED' workers which are not in
     *   the 'READ-ONLY' state will be involved into the operation.
     *
     * @param controller
     *   is needed launching requests and accessing the Configuration
     *
     * @param parentJobId
     *   (optional) identifier of a parent job
     *
     * @param onFinish
     *   (optional) callback function to be called upon a completion of the job
     *
     * @param options
     *   (optional) defines the job priority, etc.
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(uint32_t transactionId,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    AbortTransactionJob() = delete;
    AbortTransactionJob(AbortTransactionJob const&) = delete;
    AbortTransactionJob& operator=(AbortTransactionJob const&) = delete;

    ~AbortTransactionJob() final = default;

    // Trivial get methods

    uint32_t transactionId() const { return _transactionId; }

    bool allWorkers() const { return _allWorkers; }

    /**
     * Return the combined result of the operation
     *
     * @note:
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throws std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    AbortTransactionJobResult const& getResultData() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /// @see Job::notify()
    void notify(util::Lock const& lock) final;


private:
    /// @see AbortTransactionJob::create()
    AbortTransactionJob(uint32_t transactionId,
                        bool allWorkers,
                        Controller::Ptr const& controller,
                        std::string const& parentJobId,
                        CallbackType const& onFinish,
                        Job::Options const& options);


    /// Callback on the request completion
    void _onRequestFinish(SqlDeleteTablePartitionRequest::Ptr const& request);
 
    /**
     * The method will scan the input queue '_worker2tables' and submit up to
     * the specified number of requests for the worker-side processing.
     * The corresponding entries from the input queue will be removed.
     * The algorithm will try to keep the workers equally loaded.
     * If no candidates were found in the input queue then the method would return 0.
     *
     * @param lock
     *   the lock on the base class's mutex Job::_mtx to be acquired
     * 
     * @param maxRequests
     *   the maximum number of requests to be submitted
     * 
     * @return
     *   the actual number of requests to be submitted, or 0 if the input
     *   queue is empty
     */
    size_t _submitNextBatch(util::Lock const& lock,
                            size_t const maxRequests);

    // Input parameters

    uint32_t const _transactionId;
    bool     const _allWorkers;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// Set up by the constructor

    TransactionInfo _transactionInfo;
    DatabaseInfo  _databaseInfo;

    std::vector<std::string> _workers;

    /// A collection of tables to be processed. Worker names are the keys
    /// of this dictionary. Tables are removed from the dictionary as
    /// the corresponding requests are issued.
    std::map<std::string,std::list<std::string>> _worker2tables;
    
    /// A collection of requests which are either in flight, or finished
    std::list<SqlDeleteTablePartitionRequest::Ptr> _requests;

    // Request counters are used for tracking a condition for
    // completing the job and for computing its final state.

    size_t _numLaunched = 0;
    size_t _numFinished = 0;
    size_t _numSuccess = 0;

    /// The result of the operation (gets updated as requests are finishing)
    AbortTransactionJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H
