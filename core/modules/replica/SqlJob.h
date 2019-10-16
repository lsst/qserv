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
#include <tuple>

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

    /// Result sets for the workers. Note, that specific job types
    /// may launch more than one request per worker.
    std::map<std::string, std::list<SqlResultSet>> resultSets;
};

/**
 * Class SqlJob is a base class for a family of jobs which broadcast the same
 * query to all worker databases of a setup. Result sets are collected in the above
 * defined data structure.
 */
class SqlJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlJob> Ptr;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    // Default construction and copy semantics are prohibited

    SqlJob() = delete;
    SqlJob(SqlJob const&) = delete;
    SqlJob& operator=(SqlJob const&) = delete;

    ~SqlJob() override = default;

    // Trivial get methods

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

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:
    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
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
     * @param jobName
     *   the name of a job in the persistent state of the Replication system
     *
     * @param options
     *   (optional) defines the job priority, etc.
     */
    SqlJob(uint64_t maxRows,
           bool allWorkers,
           Controller::Ptr const& controller,
           std::string const& parentJobId,
           std::string const& jobName,
           Job::Options const& options);

    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /**
     * The callback function to be invoked on a completion of requests
     * targeting workers.
     */
    void onRequestFinish(SqlRequest::Ptr const& request);

    /**
     * This method lets a request type-specific subclass to launch requests
     * of the corresponding subtype.
     *
     * @param lock
     *   on the mutex Job::_mtx to be acquired for protecting the object's state
     *
     * @param worker
     *   the name of a worker the requests to be sent to
     * 
     * @param maxRequests
     *   the maximum number of requests to be launched
     *
     * @return a collection of requests launched
     */
    virtual std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                                      std::string const& worker,
                                                      size_t maxRequests=1) = 0;

    /**
     * This method lets a request type-specific subclass to stop requests
     * of the corresponding subtype.
     */
    virtual void stopRequest(util::Lock const& lock,
                             SqlRequest::Ptr const& request) = 0;

private:
    // Input parameters

    uint64_t const _maxRows;
    bool     const _allWorkers;

    /// A collection of requests implementing the operation
    std::vector<SqlRequest::Ptr> _requests;

    /// This counter is used for tracking a condition for completing the job
    /// before computing its final state.
    size_t _numFinished = 0;

    /// The result of the operation (gets updated as requests are finishing)
    SqlJobResult _resultData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLJOB_H
