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
#include <memory>
#include <string>

// Qserv headers
#include "replica/Controller.h"
#include "replica/SqlDeleteTablePartitionJob.h"
#include "replica/SqlJobResult.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class AbortTransactionJob represents a tool which will issue and track
 * a collection of the table-specific jobs for removing MySQL partitions
 * from the tables corresponding to a given super-transaction. The algorithm
 * is designed to work for all types of tables (regular or partitioned) across
 * a select (sub-)set of workers. Result sets are collected into an object
 * of class SqlJobResult.
 */
class AbortTransactionJob : public Job  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<AbortTransactionJob> Ptr;

    /// The function type for notifications on the completion of the job
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /// @return default options object for this type of a job
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
    SqlJobResult const& getResultData() const;

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:
    void startImpl(util::Lock const& lock) final;
    void cancelImpl(util::Lock const& lock) final;
    void notify(util::Lock const& lock) final;

private:
    AbortTransactionJob(TransactionId transactionId,
                        bool allWorkers,
                        Controller::Ptr const& controller,
                        std::string const& parentJobId,
                        CallbackType const& onFinish,
                        Job::Options const& options);


    void _onChildJobFinish(SqlDeleteTablePartitionJob::Ptr const& job);
 
    // Input parameters

    TransactionId const _transactionId;
    bool const _allWorkers;

    CallbackType _onFinish;     /// @note is reset when the job finishes
    
    /// A collection of the child jobs which are either in flight, or finished
    std::list<SqlDeleteTablePartitionJob::Ptr> _jobs;

    // Counters for the child jobs are used for tracking a condition for completing
    // this job and for computing its final state.

    size_t _numFinished = 0;
    size_t _numSuccess = 0;

    /// The result of the operation (gets updated as jobs are finishing)
    SqlJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H
