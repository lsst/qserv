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
#ifndef LSST_QSERV_REPLICA_SQLDELETETABLEJOB_H
#define LSST_QSERV_REPLICA_SQLDELETETABLEJOB_H

// System headers
#include <functional>
#include <list>
#include <set>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/SqlJob.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class SqlDeleteTableJob represents a tool which will broadcast the same request
 * for deleting an existing table from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be deleted from all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks found at the relevant workers.
 */
class SqlDeleteTableJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteTableJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database the name of a database from which a table will be deleted
     * @param table the name of an existing table to be deleted
     * @param allWorkers engage all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers which are not
     *   in the 'READ-ONLY' sub-state will be involved into the operation.
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId (optional) identifier of a parent job
     * @param onFinish (optional) callback function to be called upon a completion
     *   of the job
     * @param options (optional) defines the job priority, etc.
     * @return pointer to the created object
     */
    static Ptr create(std::string const& database,
                      std::string const& table,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    SqlDeleteTableJob() = delete;
    SqlDeleteTableJob(SqlDeleteTableJob const&) = delete;
    SqlDeleteTableJob& operator=(SqlDeleteTableJob const&) = delete;

    ~SqlDeleteTableJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& table()    const { return _table; }

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    void notify(util::Lock const& lock) final;

    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    SqlDeleteTableJob(std::string const& database,
                      std::string const& table,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      Job::Options const& options);

    // Input parameters

    std::string const _database;
    std::string const _table;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLDELETETABLEJOB_H
