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
#ifndef LSST_QSERV_REPLICA_SQLDROPINDEXESJOB_H
#define LSST_QSERV_REPLICA_SQLDROPINDEXESJOB_H

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
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class SqlDropIndexesJob represents a tool which will broadcast batches of
 * the table index deletion requests to workers.
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be searched at all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks associated with the corresponding
 * workers, as well as so called "dummy chunk" tables.
 */
class SqlDropIndexesJob: public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDropIndexesJob> Ptr;

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
     * @param indexName The name of the index affected by the operation.
     * @param allWorkers The flag which if set to 'true' will engage all known
     *   workers regardless of their status. If the flag is set to 'false' then
     *   only 'ENABLED' workers which are not in the 'READ-ONLY' state will be
     *   involved into the operation.
     * @param controller This is needed launching requests and accessing the Configuration.
     * @param parentJobId (optional) An identifier of a parent job.
     * @param onFinish (optional) A callback function to be called upon a completion of the job.
     * @param options (optional) A collection of parameters defining the job priority, etc.
     * @return A pointer to the created object.
     */
    static Ptr create(std::string const& database,
                      std::string const& table,
                      std::string const& indexName,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    SqlDropIndexesJob() = delete;
    SqlDropIndexesJob(SqlDropIndexesJob const&) = delete;
    SqlDropIndexesJob& operator=(SqlDropIndexesJob const&) = delete;

    ~SqlDropIndexesJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    std::string const& indexName() const { return _indexName; }

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    void notify(util::Lock const& lock) final;

    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    SqlDropIndexesJob(std::string const& database,
                      std::string const& table,
                      std::string const& indexName,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      Job::Options const& options);

    // Input parameters

    std::string const _database;
    std::string const _table;
    std::string const _indexName;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLDROPINDEXESJOB_H
