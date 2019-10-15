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
#include <set>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/Job.h"
#include "replica/SqlCreateDbRequest.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/SqlDeleteDbRequest.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlDeleteTableRequest.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/SqlEnableDbRequest.h"
#include "replica/SqlGrantAccessRequest.h"
#include "replica/SqlQueryRequest.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"
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
     * @param options
     *   (optional) defines the job priority, etc.
     */
    SqlJob(uint64_t maxRows,
           bool allWorkers,
           Controller::Ptr const& controller,
           std::string const& parentJobId,
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


/**
 * Class SqlQueryJob represents a tool which will broadcast the same query to all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 */
class SqlQueryJob : public SqlJob  {
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
                                              size_t maxRequests) final;

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
                Job::Options const& options);

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


/**
 * Class SqlCreateDbJob represents a tool which will broadcast the same request
 * for creating a new database to all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlCreateDbJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateDbJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database to be created
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
    static Ptr create(std::string const& database,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlCreateDbJob() = delete;
    SqlCreateDbJob(SqlCreateDbJob const&) = delete;
    SqlCreateDbJob& operator=(SqlCreateDbJob const&) = delete;

    ~SqlCreateDbJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlCreateDbJob::create()
    SqlCreateDbJob(std::string const& database,
                   bool allWorkers,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options);

    // Input parameters

    std::string const _database;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlDeleteDbJob represents a tool which will broadcast the same request
 * for deleting an existing database from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDeleteDbJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteDbJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database to be deleted
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
    static Ptr create(std::string const& database,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlDeleteDbJob() = delete;
    SqlDeleteDbJob(SqlDeleteDbJob const&) = delete;
    SqlDeleteDbJob& operator=(SqlDeleteDbJob const&) = delete;

    ~SqlDeleteDbJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlDeleteDbJob::create()
    SqlDeleteDbJob(std::string const& database,
                   bool allWorkers,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options);

    // Input parameters

    std::string const _database;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlEnableDbJob represents a tool which will broadcast the same request
 * for enabling an existing database at all Qserv workers of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlEnableDbJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlEnableDbJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database to be enabled
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
    static Ptr create(std::string const& database,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlEnableDbJob() = delete;
    SqlEnableDbJob(SqlEnableDbJob const&) = delete;
    SqlEnableDbJob& operator=(SqlEnableDbJob const&) = delete;

    ~SqlEnableDbJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlEnableDbJob::create()
    SqlEnableDbJob(std::string const& database,
                   bool allWorkers,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options);

    // Input parameters

    std::string const _database;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlDisableDbJob represents a tool which will broadcast the same request
 * for disabling an existing database at all Qserv workers of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDisableDbJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDisableDbJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database to be disabled
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
    static Ptr create(std::string const& database,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlDisableDbJob() = delete;
    SqlDisableDbJob(SqlDisableDbJob const&) = delete;
    SqlDisableDbJob& operator=(SqlDisableDbJob const&) = delete;

    ~SqlDisableDbJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlDisableDbJob::create()
    SqlDisableDbJob(std::string const& database,
                    bool allWorkers,
                    Controller::Ptr const& controller,
                    std::string const& parentJobId,
                    CallbackType const& onFinish,
                    Job::Options const& options);

    // Input parameters

    std::string const _database;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlGrantAccessJob represents a tool which will broadcast the same request
 * for granting access to an existing database at all Qserv workers of a setup.
 * Result sets are collected in the above defined data structure.
 */
class SqlGrantAccessJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlGrantAccessJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database to be accessed by the user
     *
     * @param user
     *   the name of a user affected by the operation
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
    static Ptr create(std::string const& database,
                      std::string const& user,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlGrantAccessJob() = delete;
    SqlGrantAccessJob(SqlGrantAccessJob const&) = delete;
    SqlGrantAccessJob& operator=(SqlGrantAccessJob const&) = delete;

    ~SqlGrantAccessJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& user()     const { return _user; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlGrantAccessJob::create()
    SqlGrantAccessJob(std::string const& database,
                      std::string const& user,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      Job::Options const& options);

    // Input parameters

    std::string const _database;
    std::string const _user;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlCreateTableJob represents a tool which will broadcast the same request
 * for creating a new table to all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlCreateTableJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateTableJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database where a new table will be created
     *
     * @param table
     *   the name of a table to be created
     *
     * @param engine
     *   the name of the MySQL engine for the new table
     *
     * @pram partitionByColumn
     *   (optional, if not empty) the name of a column which will be used
     *   as a key to configure MySQL partitions for the new table.
     *   This variation of table schema will be used for the super-transaction-based
     *   ingest into the table.
     *
     * @param columns
     *   column definitions as pairs of (name,type) of the table
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
    static Ptr create(std::string const& database,
                      std::string const& table,
                      std::string const& engine,
                      std::string const& partitionByColumn,
                      std::list<std::pair<std::string, std::string>> const& columns,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlCreateTableJob() = delete;
    SqlCreateTableJob(SqlCreateTableJob const&) = delete;
    SqlCreateTableJob& operator=(SqlCreateTableJob const&) = delete;

    ~SqlCreateTableJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& table()    const { return _table; }
    std::string const& engine()   const { return _engine; }

    std::string const& partitionByColumn() const { return _partitionByColumn; }

    std::list<std::pair<std::string, std::string>> const& columns() const { return _columns; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlCreateTableJob::create()
    SqlCreateTableJob(std::string const& database,
                      std::string const& table,
                      std::string const& engine,
                      std::string const& partitionByColumn,
                      std::list<std::pair<std::string, std::string>> const& columns,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      Job::Options const& options);

    // Input parameters

    std::string const _database;
    std::string const _table;
    std::string const _engine;
    std::string const _partitionByColumn;

    std::list<std::pair<std::string, std::string>> const _columns;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};


/**
 * Class SqlDeleteTableJob represents a tool which will broadcast the same request
 * for deleting an existing table from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDeleteTableJob : public SqlJob  {
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
     * @param database
     *   the name of a database from which a table will be deleted
     *
     * @param table
     *   the name of an existing table to be deleted
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
    static Ptr create(std::string const& database,
                      std::string const& table,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlDeleteTableJob() = delete;
    SqlDeleteTableJob(SqlDeleteTableJob const&) = delete;
    SqlDeleteTableJob& operator=(SqlDeleteTableJob const&) = delete;

    ~SqlDeleteTableJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& table()    const { return _table; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlDeleteTableJob::create()
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


/**
 * Class SqlRemoveTablePartitionsJob represents a tool which will broadcast
 * the same request for removing MySQL partitions from existing table from all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 * 
 * Note, that the algorithm treats regular and partitioned tables quite differently.
 * For the regular tables it will indeed broadcast exactly the same request
 * (to the exact table specified as the corresponding parameter of the job)
 * to all workers. The regular tables must be present at all workers.
 * The partitioned (chunked) tables will be treated quite differently. First of
 * all, the name of a table specified as a parameter of the class will be treated
 * as a class of the tables, and a group of table-specific AND(!) chunk-specific
 * requests will be generated for such table. For example, of the table name is:
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
class SqlRemoveTablePartitionsJob : public SqlJob  {
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
     * @param database
     *   the name of a database from which a table will be deleted
     *
     * @param table
     *   the name of an existing table to be affected by the operation
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
    static Ptr create(std::string const& database,
                      std::string const& table,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlRemoveTablePartitionsJob() = delete;
    SqlRemoveTablePartitionsJob(SqlRemoveTablePartitionsJob const&) = delete;
    SqlRemoveTablePartitionsJob& operator=(SqlRemoveTablePartitionsJob const&) = delete;

    ~SqlRemoveTablePartitionsJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& table()    const { return _table; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlRemoveTablePartitionsJob::create()
    SqlRemoveTablePartitionsJob(std::string const& database,
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

    /// Is set in the constructor by pulling table status from the Configuration
    bool _isPartitioned = false;

    /// A collection of per-worker tables for which the remote operations are
    /// required. Each worker-specific sub-collections gets initialized
    /// just once upon the very first request to the request launching method
    /// in a context of the corresponding worker. Hence there are three states
    /// of the sub-collections:
    ///
    /// - (initial) no worker key exists. At this state the algorithm would
    ///   initialize the sub-collection if called at the request launching method
    ///   for the first time in a context of the worker.
    /// - (populated) will be used for making requests to the worker. Each time
    ///   a request for a table is sent to the worker the table gets removed from
    ///   from the sub-collection
    /// - (empty) the worker key exists. This means no tables to be processed for
    ///   by the worker exists. The tables have either been all processed, or
    ///   the collection was made empty upon the initialization.
    std::map<std::string, std::list<std::string>> _workers2tables;
};


/**
 * Class SqlDeleteTablePartitionJob represents a tool which will broadcast
 * the same request for removing a MySQL partition corresponding to a given
 * super-transaction from existing table from all worker databases of a setup.
 * Result sets are collected in the above defined data structure.
 */
class SqlDeleteTablePartitionJob : public SqlJob  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteTablePartitionJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database
     *   the name of a database from which a table will be deleted
     *
     * @param table
     *   the name of an existing table to be affected by the operation
     *
     * @param transactionId
     *   an identifier of a super-transaction corresponding to a MySQL partition
     *   to be dropped. The transaction must exist, and it should be in
     *   the ABORTED state.
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
    static Ptr create(std::string const& database,
                      std::string const& table,
                      uint32_t transactionId,
                      bool allWorkers,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    SqlDeleteTablePartitionJob() = delete;
    SqlDeleteTablePartitionJob(SqlDeleteTablePartitionJob const&) = delete;
    SqlDeleteTablePartitionJob& operator=(SqlDeleteTablePartitionJob const&) = delete;

    ~SqlDeleteTablePartitionJob() final = default;

    // Trivial get methods

    std::string const& database() const { return _database; }
    std::string const& table()    const { return _table; }

    uint32_t transactionId() const { return _transactionId; }


    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:
    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

    /// @see SqlJob::launchRequests()
    std::list<SqlRequest::Ptr> launchRequests(util::Lock const& lock,
                                              std::string const& worker,
                                              size_t maxRequests) final;

    /// @see SqlJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlRequest::Ptr const& request) final;

private:
    /// @see SqlDeleteTablePartitionJob::create()
    SqlDeleteTablePartitionJob(std::string const& database,
                               std::string const& table,
                               uint32_t transactionId,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               std::string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options);

    // Input parameters

    std::string const _database;
    std::string const _table;
    uint32_t    const _transactionId;

    CallbackType _onFinish;     /// @note is reset when the job finishes

    /// A registry of workers to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::set<std::string> _workers;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLJOB_H
