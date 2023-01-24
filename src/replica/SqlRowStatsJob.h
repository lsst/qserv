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
#ifndef LSST_QSERV_REPLICA_SQLROWSTATSJOB_H
#define LSST_QSERV_REPLICA_SQLROWSTATSJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <set>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/SqlJob.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlRowStatsJob represents a tool which will broadcast batches of
 * the table row counter retrieval requests to workers.
 *
 * The algorithm has an option of updating the persistnet state of the system
 * where it retains counters of the last scan. This state can be updated if the scan
 * was successfull (unless the algorithm is run in the StateUpdatePolicy::FORCED mode)
 * and if the counters match across all replicas of the same table.
 * In the StateUpdatePolicy::FORCED was selected then updates will be made for
 * the successfully scanned tables only.
 * @see SqlRowStatsJob::stateUpdatePolicy()
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be searched at all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks associated with the corresponding
 * workers, as well as so called "dummy chunk" tables.
 */
class SqlRowStatsJob : public SqlJob {
public:
    typedef std::shared_ptr<SqlRowStatsJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /// Options for updating the persistent state of the table counters.
    enum StateUpdatePolicy {
        DISABLED,
        ENABLED,  ///< Only if the scan succeeded.
        FORCED    ///< Only for the successfully scanned tables.
    };

    /// @return The string representation of the policy.
    static std::string policy2str(StateUpdatePolicy policy);

    /// @return The parsed policy.
    /// @throws std::invalid_argument If the string didn't match any known policy.
    static StateUpdatePolicy str2policy(std::string const& str);

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database The name of a database where the tables are residing.
     * @param table The name of the base table to be affected by the operation.
     * @param overlapSelector The selector (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be affected by
     *   the operation.
     * @param stateUpdatePolicy The option that tells the job if it should also
     *   update row counters in the persistent state of the Replication system.
     * @param allWorkers The flag which if set to 'true' will engage all known
     *   workers regardless of their status. If the flag is set to 'false' then
     *   only 'ENABLED' workers which are not in the 'READ-ONLY' state will be
     *   involved into the operation.
     * @param controller This is needed launching requests and accessing the Configuration.
     * @param parentJobId An identifier of a parent job.
     * @param onFinish A callback function to be called upon a completion of the job.
     * @param priority The priority level of the job.
     * @return A pointer to the created object.
     */
    static Ptr create(std::string const& database, std::string const& table,
                      ChunkOverlapSelector overlapSelector, StateUpdatePolicy stateUpdatePolicy,
                      bool allWorkers, Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    SqlRowStatsJob() = delete;
    SqlRowStatsJob(SqlRowStatsJob const&) = delete;
    SqlRowStatsJob& operator=(SqlRowStatsJob const&) = delete;

    ~SqlRowStatsJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    ChunkOverlapSelector overlapSelector() const { return _overlapSelector; }
    StateUpdatePolicy stateUpdatePolicy() const { return _stateUpdatePolicy; }

    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    void notify(replica::Lock const& lock) final;

    std::list<SqlRequest::Ptr> launchRequests(replica::Lock const& lock, std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

    void stopRequest(replica::Lock const& lock, SqlRequest::Ptr const& request) final;
    void processResultAndFinish(replica::Lock const& lock, ExtendedState extendedState) final;

private:
    SqlRowStatsJob(std::string const& database, std::string const& table,
                   ChunkOverlapSelector overlapSelector, StateUpdatePolicy stateUpdatePolicy, bool allWorkers,
                   Controller::Ptr const& controller, std::string const& parentJobId,
                   CallbackType const& onFinish, int priority);

    /**
     * @brief Process a result set and (in case of success) update the collection of counters.
     *
     * @param context_ The prefix for reporting errors.
     * @param isPartitioned The kind of a partitioned table.
     * @param worker The name of a worker.
     * @param internalTable The name of the internal (at the worker) table.
     * @param resultSet A result set to be analyzed.
     * @param counters The collection to be updated.
     * @return true If succeeded.
     * @return false If failed.
     */
    bool _process(std::string const& context_, bool isPartitioned, SqlJobResult::Worker const& worker,
                  SqlJobResult::Scope const& internalTable, SqlResultSet::ResultSet const& resultSet,
                  std::map<std::string, std::map<TransactionId, std::vector<size_t>>>& counters) const;

    // Input parameters

    std::string const _database;
    std::string const _table;
    ChunkOverlapSelector const _overlapSelector;
    StateUpdatePolicy const _stateUpdatePolicy;

    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLROWSTATSJOB_H
