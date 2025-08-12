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
#ifndef LSST_QSERV_REPLICA_SQLCREATETABLESJOB_H
#define LSST_QSERV_REPLICA_SQLCREATETABLESJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <set>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/jobs/SqlJob.h"
#include "replica/util/Common.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlCreateTablesJob represents a tool which will broadcast batches of
 * the table creation requests to workers.
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be created at all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks associated with the corresponding
 * workers, as well as so called "dummy chunk" tables.
 */
class SqlCreateTablesJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateTablesJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database The name of a database from which a table will be created.
     * @param table The name of the base table to be affected by the operation.
     * @param engine The name of a MySQL engine for the tables.
     * @param partitionByColumn The name of a column to be used for the MySQL partitioning.
     *   The partitioning won't me made if a value of the parameter is an empty string.
     * @param columns The table schema.
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
    static Ptr create(std::string const& database, std::string const& table, std::string const& engine,
                      std::string const& partitionByColumn, std::list<SqlColDef> const& columns,
                      std::string const& charsetName, std::string const& collationName, bool allWorkers,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    SqlCreateTablesJob() = delete;
    SqlCreateTablesJob(SqlCreateTablesJob const&) = delete;
    SqlCreateTablesJob& operator=(SqlCreateTablesJob const&) = delete;

    ~SqlCreateTablesJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    std::string const& engine() const { return _engine; }
    std::string const& partitionByColumn() const { return _partitionByColumn; }
    std::list<SqlColDef> const& columns() const { return _columns; }
    std::string const& charsetName() const { return _charsetName; }
    std::string const& collationName() const { return _collationName; }
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    void notify(replica::Lock const& lock) final;
    std::list<SqlRequest::Ptr> launchRequests(replica::Lock const& lock, std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

private:
    SqlCreateTablesJob(std::string const& database, std::string const& table, std::string const& engine,
                       std::string const& partitionByColumn, std::list<SqlColDef> const& columns,
                       std::string const& charsetName, std::string const& collationName, bool allWorkers,
                       Controller::Ptr const& controller, std::string const& parentJobId,
                       CallbackType const& onFinish, int priority);

    // Input parameters
    std::string const _database;
    std::string const _table;
    std::string const _engine;
    std::string const _partitionByColumn;
    std::list<SqlColDef> const _columns;
    std::string const _charsetName;
    std::string const _collationName;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLCREATETABLESJOB_H
