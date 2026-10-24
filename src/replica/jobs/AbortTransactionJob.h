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
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <utility>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/jobs/SqlJobResult.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class SqlDeleteTablePartitionJob;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class AbortTransactionJob represents a tool which will issue and track
 * a collection of the table-specific jobs for removing MySQL partitions
 * from the tables corresponding to a given super-transaction. The algorithm
 * is designed to work for all types of tables (regular or partitioned) across
 * a select (sub-)set of workers. Result sets are collected into an object
 * of class SqlJobResult.
 *
 * @note Only unpublished tables will be affected by the job.
 */
class AbortTransactionJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<AbortTransactionJob> Ptr;

    /// The function type for notifications on the completion of the job
    typedef std::function<void(std::shared_ptr<AbortTransactionJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan and memory management
     * of instances created otherwise (as values or via low-level pointers).
     *
     * @param transactionId an identifier of a super-transaction corresponding to a MySQL partition
     *  to be dropped. The transaction must exist, and it should be in the ABORTED state.
     * @param allWorkers engage all known workers regardless of their status. If the flag is set to 'false'
     *  then only 'ENABLED' workers which are not in the 'READ-ONLY' state will be involved into the
     * operation.
     * @param controller is needed launching requests and accessing the Config
     * @param parentJobId an identifier of the parent job
     * @param onFinish A callback function to be called upon a completion of the job
     * @param priority the the priority level of the job
     * @return A pointer to the created object
     */
    static std::shared_ptr<AbortTransactionJob> create(TransactionId transactionId, bool allWorkers,
                                                       std::shared_ptr<Controller> const& controller,
                                                       std::string const& parentJobId,
                                                       CallbackType const& onFinish, int priority);

    AbortTransactionJob() = delete;
    AbortTransactionJob(AbortTransactionJob const&) = delete;
    AbortTransactionJob& operator=(AbortTransactionJob const&) = delete;

    virtual ~AbortTransactionJob() final = default;

    TransactionId transactionId() const { return _transactionId; }
    bool allWorkers() const { return _allWorkers; }

    virtual Job::Progress progress() const override;

    /**
     * Return the combined result of the operation
     *
     * @note The method should be invoked only after the job has finished (primary status is set
     *  to Job::Status::FINISHED). Otherwise exception std::logic_error will be thrown.
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when the method was called
     */
    SqlJobResult const& getResultData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    AbortTransactionJob(TransactionId transactionId, bool allWorkers,
                        std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                        CallbackType const& onFinish, int priority);

    void _onChildJobFinish(std::shared_ptr<SqlDeleteTablePartitionJob> const& job);

    // Input parameters

    TransactionId const _transactionId;
    bool const _allWorkers;

    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// A collection of the child jobs which are either in flight, or finished
    std::list<std::shared_ptr<SqlDeleteTablePartitionJob>> _jobs;

    // Counters for the child jobs are used for tracking a condition for completing
    // this job and for computing its final state.

    std::size_t _numFinished = 0;
    std::size_t _numSuccess = 0;

    /// The result of the operation (gets updated as jobs are finishing)
    SqlJobResult _resultData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_ABORTTRANSACTIONJOB_H
