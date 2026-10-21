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
#ifndef LSST_QSERV_REPLICA_SQLDELETEDBJOB_H
#define LSST_QSERV_REPLICA_SQLDELETEDBJOB_H

// System headers
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/jobs/SqlJob.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
class HttpSqlRequest;
#else
class SqlRequest;
#endif
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlDeleteDbJob represents a tool which will broadcast the same request
 * for deleting an existing database from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDeleteDbJob : public SqlJob {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<SqlDeleteDbJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Create the job.
     *
     * @param database the name of a database to be deleted
     * @param allWorkers engage all known workers regardless of their status. If the flag
     *  is set to 'false' then only 'ENABLED' workers which are not in the 'READ-ONLY' state
     *  will be involved into the operation.
     * @param controller is needed launching requests and accessing the Config
     * @param parentJobId an identifier of a parent job
     * @param onFinish a callback function to be called upon a completion of the job
     * @param priority defines the job priority
     * @return pointer to the created object
     */
    static std::shared_ptr<SqlDeleteDbJob> create(std::string const& database, bool allWorkers,
                                                  std::shared_ptr<Controller> const& controller,
                                                  std::string const& parentJobId,
                                                  CallbackType const& onFinish, int priority);

    SqlDeleteDbJob() = delete;
    SqlDeleteDbJob(SqlDeleteDbJob const&) = delete;
    SqlDeleteDbJob& operator=(SqlDeleteDbJob const&) = delete;

    virtual ~SqlDeleteDbJob() final = default;

    std::string const& database() const { return _database; }

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    virtual void notify(replica::Lock const& lock) final;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
    virtual std::list<std::shared_ptr<HttpSqlRequest>> launchRequests(replica::Lock const& lock,
                                                                      std::string const& worker,
                                                                      std::size_t maxRequestsPerWorker) final;
#else
    virtual std::list<std::shared_ptr<SqlRequest>> launchRequests(replica::Lock const& lock,
                                                                  std::string const& worker,
                                                                  std::size_t maxRequestsPerWorker) final;
#endif

private:
    SqlDeleteDbJob(std::string const& database, bool allWorkers,
                   std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                   CallbackType const& onFinish, int priority);

    // Input parameters
    std::string const _database;
    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLDELETEDBJOB_H
