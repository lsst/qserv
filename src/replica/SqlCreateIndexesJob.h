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
#ifndef LSST_QSERV_REPLICA_SQLCREATEINDEXESJOB_H
#define LSST_QSERV_REPLICA_SQLCREATEINDEXESJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <set>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/Common.h"
#include "replica/SqlJob.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class SqlCreateIndexesJob represents a tool which will broadcast batches of
 * the table index creation requests to workers.
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be searched at all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks associated with the corresponding
 * workers, as well as so called "dummy chunk" tables.
 */
class SqlCreateIndexesJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateIndexesJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database The name of a database where the tables are residing.
     * @param table The name of the base table to be affected by the operation.
     * @param overlap The flag (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be affected by
     *   the operation. If the flag is set to 'true' then only the overlap tables
     *   will be involved into the operation. Otherwise, only the chunk tables will
     *   be affected.
     * @param indexSpec The type specification of the index.
     * @param indexName The name of the index.
     * @param indexComment An arbitrary comment string documenting the index.
     * @param indexColumns Column definitions (name,length,ordering) for the index.
     * @param allWorkers The flag which if set to 'true' will engage all known
     *   workers regardless of their status. If the flag is set to 'false' then
     *   only 'ENABLED' workers which are not in the 'READ-ONLY' state will be
     *   involved into the operation.
     * @param ignoreDuplicateKey The flag which if 'true' then don't report as
     *   errors tables for which ProtocolStatusExt::DUPLICATE_KEY was reported.
     *   The flag can be useful for tables in which the index may already exist.
     * @param controller This is needed launching requests and accessing the Configuration.
     * @param parentJobId An identifier of a parent job.
     * @param onFinish A callback function to be called upon a completion of the job.
     * @param priority The priority level of the job.
     * @return A pointer to the created object.
     */
    static Ptr create(std::string const& database, std::string const& table, bool overlap,
                      SqlRequestParams::IndexSpec const& indexSpec, std::string const& indexName,
                      std::string const& indexComment, std::vector<SqlIndexColumn> const& indexColumns,
                      bool allWorkers, bool ignoreDuplicateKey, Controller::Ptr const& controller,
                      std::string const& parentJobId, CallbackType const& onFinish, int priority);

    SqlCreateIndexesJob() = delete;
    SqlCreateIndexesJob(SqlCreateIndexesJob const&) = delete;
    SqlCreateIndexesJob& operator=(SqlCreateIndexesJob const&) = delete;

    ~SqlCreateIndexesJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    bool overlap() const { return _overlap; }
    SqlRequestParams::IndexSpec const& indexSpec() const { return _indexSpec; }
    std::string const& indexName() const { return _indexName; }
    std::string const& indexComment() const { return _indexComment; }
    std::vector<SqlIndexColumn> const& indexColumns() const { return _indexColumns; }

    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    void notify(util::Lock const& lock) final;

    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock, std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

    void stopRequest(util::Lock const& lock, SqlRequest::Ptr const& request) final;

private:
    SqlCreateIndexesJob(std::string const& database, std::string const& table, bool overlap,
                        SqlRequestParams::IndexSpec const& indexSpec, std::string const& indexName,
                        std::string const& indexComment, std::vector<SqlIndexColumn> const& indexColumns,
                        bool allWorkers, bool ignoreDuplicateKey, Controller::Ptr const& controller,
                        std::string const& parentJobId, CallbackType const& onFinish, int priority);

    // Input parameters

    std::string const _database;
    std::string const _table;
    bool const _overlap;
    SqlRequestParams::IndexSpec const _indexSpec;
    std::string const _indexName;
    std::string const _indexComment;
    std::vector<SqlIndexColumn> const _indexColumns;

    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLCREATETABLESJOB_H
