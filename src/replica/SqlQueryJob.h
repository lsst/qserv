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
#ifndef LSST_QSERV_REPLICA_SQLQUERYJOB_H
#define LSST_QSERV_REPLICA_SQLQUERYJOB_H

// System headers
#include <cstdint>
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
 * Class SqlQueryJob represents a tool which will broadcast the same query to all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 */
class SqlQueryJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlQueryJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param query
     *   the query to be executed on all workers
     *
     * @param user
     *   the name of a database account for connecting to the database service
     *
     * @param password
     *   a database for connecting to the database service
     *
     * @param maxRows
     *   (optional) limit for the maximum number of rows to be returned with the request.
     *   Leaving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. Note that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     *
     * @param allWorkers
     *   engage all known workers regardless of their status. If the flag
     *   is set to 'false' then only 'ENABLED' workers which are not in
     *   the 'READ-ONLY' state will be involved into the operation.
     *
     * @param controller
     *   is needed launching requests and accessing the Configuration
     *
     * @param parentJobId
     *   an identifier of the parent job
     *
     * @param onFinish
     *   a callback function to be called upon a completion of the job
     *
     * @param priority
     *   defines the job priority
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(std::string const& query,
                      std::string const& user,
                      std::string const& password,
                      uint64_t maxRows,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      int priority);

    // Default construction and copy semantics are prohibited

    SqlQueryJob() = delete;
    SqlQueryJob(SqlQueryJob const&) = delete;
    SqlQueryJob& operator=(SqlQueryJob const&) = delete;

    ~SqlQueryJob() final = default;

    // Trivial get methods

    std::string const& query()    const { return _query; }
    std::string const& user()     const { return _user; }
    std::string const& password() const { return _password; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlQueryJob::create()
    SqlQueryJob(std::string const& query,
                std::string const& user,
                std::string const& password,
                uint64_t maxRows,
                bool allWorkers,
                Controller::Ptr const& controller,
                std::string const& parentJobId,
                CallbackType const& onFinish,
                int priority);

    // Input parameters

    std::string const _query;
    std::string const _user;
    std::string const _password;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLQUERYJOB_H
