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
 * Class SqlBaseJob is a base class for a family of jobs which broadcast the same
 * query to all worker databases of a setup. Result sets are collected in the above
 * defined data structure.
 */
class SqlBaseJob : public Job  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlBaseJob> Ptr;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    // Default construction and copy semantics are prohibited

    SqlBaseJob() = delete;
    SqlBaseJob(SqlBaseJob const&) = delete;
    SqlBaseJob& operator=(SqlBaseJob const&) = delete;

    ~SqlBaseJob() override = default;

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
    SqlBaseJob(uint64_t maxRows,
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
    void onRequestFinish(SqlBaseRequest::Ptr const& request);

    /**
     * This method lets a request type-specific subclass to launch requests
     * of the corresponding subtype.
     */
    virtual SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                              std::string const& worker) = 0;

    /**
     * This method lets a request type-specific subclass to stop requests
     * of the corresponding subtype.
     */
    virtual void stopRequest(util::Lock const& lock,
                             SqlBaseRequest::Ptr const& request) = 0;

private:
    // Input parameters

    uint64_t const _maxRows;
    bool     const _allWorkers;

    /// A collection of requests implementing the operation
    std::vector<SqlBaseRequest::Ptr> _requests;

    // Request counters are used for tracking a condition for
    // completing the job and for computing its final state.

    size_t _numLaunched = 0;
    size_t _numFinished = 0;
    size_t _numSuccess = 0;

    /// The result of the operation (gets updated as requests are finishing)
    SqlJobResult _resultData;
};


/**
 * Class SqlQueryJob represents a tool which will broadcast the same query to all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 */
class SqlQueryJob : public SqlBaseJob  {
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlCreateDbJob represents a tool which will broadcast the same request
 * for creating a new database to all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlCreateDbJob : public SqlBaseJob  {
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
    static Ptr create(std::string const& qdatabase,
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlDeleteDbJob represents a tool which will broadcast the same request
 * for deleting an existing database from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDeleteDbJob : public SqlBaseJob  {
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
    static Ptr create(std::string const& qdatabase,
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlEnableDbJob represents a tool which will broadcast the same request
 * for enabling an existing database at all Qserv workers of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlEnableDbJob : public SqlBaseJob  {
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
    static Ptr create(std::string const& qdatabase,
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlDisableDbJob represents a tool which will broadcast the same request
 * for disabling an existing database at all Qserv workers of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDisableDbJob : public SqlBaseJob  {
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
    static Ptr create(std::string const& qdatabase,
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlCreateTableJob represents a tool which will broadcast the same request
 * for creating a new table to all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlCreateTableJob : public SqlBaseJob  {
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlDeleteTableJob represents a tool which will broadcast the same request
 * for deleting an existing table from all worker databases of a setup. Result sets
 * are collected in the above defined data structure.
 */
class SqlDeleteTableJob : public SqlBaseJob  {
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlRemoveTablePartitionsJob represents a tool which will broadcast
 * the same request for removing MySQL partitions from existing table from all
 * worker databases of a setup. Result sets are collected in the above defined
 * data structure.
 */
class SqlRemoveTablePartitionsJob : public SqlBaseJob  {
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};


/**
 * Class SqlDeleteTablePartitionJob represents a tool which will broadcast
 * the same request for removing a MySQL partition corresponding to a given
 * super-transaction from existing table from all worker databases of a setup.
 * Result sets are collected in the above defined data structure.
 */
class SqlDeleteTablePartitionJob : public SqlBaseJob  {
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

    /// @see SqlBaseJob::launchRequest()
    SqlBaseRequest::Ptr launchRequest(util::Lock const& lock,
                                      std::string const& worker) final;

    /// @see SqlBaseJob::stopRequest()
    void stopRequest(util::Lock const& lock,
                     SqlBaseRequest::Ptr const& request) final;

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
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLJOB_H
