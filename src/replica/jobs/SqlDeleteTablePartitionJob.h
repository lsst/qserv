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
#ifndef LSST_QSERV_REPLICA_SQLDELETETABLEPARTITIONJOB_H
#define LSST_QSERV_REPLICA_SQLDELETETABLEPARTITIONJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/jobs/SqlJob.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class SqlRequest;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlDeleteTablePartitionJob represents a tool that will broadcast
 * the same request for removing a MySQL partition corresponding to a given
 * super-transaction from existing table from the relevant worker databases of a setup.
 * Result sets are collected in the above defined data structure.
 *
 * @note The meaning of the 'table' depends on a kind of a table. If this is
 *   a regular table then tables with exact names will be deleted from all workers.
 *   For the partitioned tables the operation will include both the prototype
 *   tables (tables at exactly the specified name existing at all workers) and
 *   the corresponding chunk tables for all chunks found at the relevant workers.
 */
class SqlDeleteTablePartitionJob : public SqlJob {
public:
    typedef std::shared_ptr<SqlDeleteTablePartitionJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<SqlDeleteTablePartitionJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan and memory management
     * of instances created otherwise (as values or via low-level pointers).
     *
     * @param transactionId an identifier of a super-transaction corresponding to a MySQL partition
     *  to be dropped. The transaction must exist, and it should be in the ABORTED state.
     * @param table the name of an existing table to be affected by the operation
     * @param allWorkers engage all known workers regardless of their status. If the flag is set
     *  to 'false' then only 'ENABLED' workers which are not in the 'READ-ONLY' state will be involved
     *  into the operation.
     * @param controller is needed launching requests and accessing the Config
     * @param parentJobId an identifier of a parent job
     * @param onFinish a callback function to be called upon a completion of the job
     * @param priority defines the job priority
     * @return A pointer to the created object
     */
    static std::shared_ptr<SqlDeleteTablePartitionJob> create(TransactionId transactionId,
                                                              std::string const& table, bool allWorkers,
                                                              std::shared_ptr<Controller> const& controller,
                                                              std::string const& parentJobId,
                                                              CallbackType const& onFinish, int priority);

    SqlDeleteTablePartitionJob() = delete;
    SqlDeleteTablePartitionJob(SqlDeleteTablePartitionJob const&) = delete;
    SqlDeleteTablePartitionJob& operator=(SqlDeleteTablePartitionJob const&) = delete;

    virtual ~SqlDeleteTablePartitionJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    TransactionId transactionId() const { return _transactionId; }

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    virtual void notify(replica::Lock const& lock) final;
    virtual std::list<std::shared_ptr<SqlRequest>> launchRequests(replica::Lock const& lock,
                                                                  std::string const& worker,
                                                                  std::size_t maxRequestsPerWorker) final;

private:
    SqlDeleteTablePartitionJob(TransactionId transactionId, std::string const& table, bool allWorkers,
                               std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                               CallbackType const& onFinish, int priority);

    // Input parameters
    TransactionId const _transactionId;
    std::string const _table;
    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// The name is extracted from the TransactionInfo for the transaction.
    std::string _database;

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLDELETETABLEPARTITIONJOB_H
