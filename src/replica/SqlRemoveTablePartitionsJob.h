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
#ifndef LSST_QSERV_REPLICA_SQLREMOVETABLEPARTITIONSJOB_H
#define LSST_QSERV_REPLICA_SQLREMOVETABLEPARTITIONSJOB_H

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
 * Class SqlRemoveTablePartitionsJob represents a tool that will broadcast
 * the same request for removing MySQL partitions from existing table from all
 * worker databases of a setup.
 * 
 * Note, that the algorithm treats regular and partitioned tables quite differently.
 * For the regular tables it will indeed broadcast exactly the same request
 * (to the exact table specified as the corresponding parameter of the job)
 * to all workers. The regular tables must be present at all workers.
 * The partitioned (chunked) tables will be treated quite differently. First of
 * all, the name of a table specified as a parameter of the class will be treated
 * as a class of the tables, and a group of table-specific AND(!) chunk-specific
 * requests will be generated for such table. For example, if the table name is:
 *
 *   'Object'
 * 
 * and the following table replicas existed for the table at a time of the request:
 * 
 *    worker | chunk
 *   --------+-----------------------
 *      A    |  123
 *   --------+-----------------------
 *      B    |  234
 *   --------+-----------------------
 *      C    |  234
 *      D    |  345
 *
 * then the low-level requests will be sent for the following tables to
 * the corresponding workers:
 * 
 *    worker | table
 *   --------+-----------------------
 *      A    | Object
 *      A    | Object_123
 *      A    | ObjectFullOverlap_123
 *   --------+-----------------------
 *      B    | Object
 *      B    | Object_234
 *      B    | ObjectFullOverlap_234
 *   --------+-----------------------
 *      C    | Object
 *      C    | Object_234
 *      C    | ObjectFullOverlap_234
 *   --------+-----------------------
 *      D    | Object
 *      D    | Object_345
 *      D    | ObjectFullOverlap_345
 */
class SqlRemoveTablePartitionsJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlRemoveTablePartitionsJob> Ptr;

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
     * @param table the name of an existing table to be affected by the operation
     * @param allWorkers engage all known workers regardless of their status.
     *   If the flag is set to 'false' then only 'ENABLED' workers that are
     *   not in the 'READ-ONLY' sub-state will be involved into the operation.
     * @param ignoreNonPartitioned if 'true' then don't report as errors tables
     *   that don't have MySQL partitions. Those partitions may have already been
     *   removed by a previous attempt to run this algorithm. 
     * @param controller is needed launching requests and accessing the Configuration
     * @param parentJobId an identifier of the parent job
     * @param onFinish a callback function to be called upon a completion
     *   of the job
     * @param priority defines the job priority
     * @return a pointer to the created object
     */
    static Ptr create(std::string const& database,
                      std::string const& table,
                      bool allWorkers,
                      bool ignoreNonPartitioned,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      int priority);

    SqlRemoveTablePartitionsJob() = delete;
    SqlRemoveTablePartitionsJob(SqlRemoveTablePartitionsJob const&) = delete;
    SqlRemoveTablePartitionsJob& operator=(SqlRemoveTablePartitionsJob const&) = delete;

    ~SqlRemoveTablePartitionsJob() final = default;

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
    SqlRemoveTablePartitionsJob(std::string const& database,
                                std::string const& table,
                                bool allWorkers,
                                bool ignoreNonPartitioned,
                                Controller::Ptr const& controller,
                                std::string const& parentJobId,
                                CallbackType const& onFinish,
                                int priority);

    // Input parameters

    std::string const _database;
    std::string const _table;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark the ones that has been processed.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLREMOVETABLEPARTITIONSJOB_H
