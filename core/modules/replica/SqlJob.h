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
#ifndef LSST_QSERV_REPLICA_SQLJOB_H
#define LSST_QSERV_REPLICA_SQLJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Job.h"
#include "replica/SqlRequest.h"
#include "replica/SqlResultSet.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure SqlJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct SqlJobResult {

    /// Per-worker flag indicating if the query has succeeded at the worker
    std::map<std::string, bool> workers;

    /// Result sets for the workers
    std::map<std::string, SqlResultSet> resultSets;
};

/**
 * Class SqlJob represents a tool which will broadcast the same query to all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 */
class SqlJob : public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

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
     *   Laving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. NOte that other, resource-defined
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
     *   (optional) identifier of a parent job
     *
     * @param onFinish
     *   (optional) callback function to be called upon a completion of the job
     *
     * @param options
     *   (optional) defines the job priority, etc.
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
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlJob() = delete;
    SqlJob(SqlJob const&) = delete;
    SqlJob& operator=(SqlJob const&) = delete;

    ~SqlJob() final = default;

    // Trivial get methods

    std::string const& query()    const { return _query; }
    std::string const& user()     const { return _user; }
    std::string const& password() const { return _password; }

    uint64_t maxRows() const { return _maxRows; }

    bool allWorkers() const { return _allWorkers; }

    /**
     * Return the combined result of the operation
     *
     * @note:
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throws std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    SqlJobResult const& getResultData() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlJob::create()
    SqlJob(std::string const& query,
           std::string const& user,
           std::string const& password,
           uint64_t maxRows,
           bool allWorkers,
           Controller::Ptr const& controller,
           std::string const& parentJobId,
           CallbackType const& onFinish,
           Job::Options const& options);

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void _onRequestFinish(SqlRequest::Ptr const& request);


    // Input parameters

    std::string  const _query;
    std::string  const _user;
    std::string  const _password;
    uint64_t     const _maxRows;
    bool         const _allWorkers;
    CallbackType       _onFinish;       /// @note is reset when the job finishes

    /// A collection of requests implementing the operation
    std::vector<SqlRequest::Ptr> _requests;

    // Request counters are used for tracking a condition for
    // completing the job and for computing its final state.

    size_t _numLaunched;
    size_t _numFinished;
    size_t _numSuccess;

    /// The result of the operation (gets updated as requests are finishing)
    SqlJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLJOB_H
