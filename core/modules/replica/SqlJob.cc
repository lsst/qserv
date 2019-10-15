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

// Class header
#include "replica/SqlJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& SqlJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        false,  /* exclusive */
        true    /* preemptable */
    };
    return options;
}


SqlJob::SqlJob(uint64_t maxRows,
               bool allWorkers,
               Controller::Ptr const& controller,
               string const& parentJobId,
               std::string const& jobName,
               Job::Options const& options)
    :   Job(controller, parentJobId, jobName, options),
        _maxRows(maxRows),
        _allWorkers(allWorkers) {
}


SqlJobResult const& SqlJob::getResultData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(
            "SqlJob::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<pair<string,string>> SqlJob::persistentLogData() const {

    list<pair<string,string>> result;

    auto const& resultData = getResultData();

    // Per-worker stats

    for (auto&& itr: resultData.resultSets) {
        auto&& worker = itr.first;
        string workerResultSetStr;
        auto&& workerResultSet = itr.second;
        for (auto&& resultSet: workerResultSet) {
            workerResultSetStr +=
                "(char_set_name=" + resultSet.charSetName +
                ",has_result=" + string(resultSet.hasResult ? "1" : "0") +
                ",fields=" + to_string(resultSet.fields.size()) +
                ",rows=" + to_string(resultSet.rows.size()) +
                ",error=" + resultSet.error +
                "),";
        }
        result.emplace_back(
            "worker-stats",
            "worker=" + worker + ",result-set=" + workerResultSetStr
        );
    }
    return result;
}


void SqlJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const workerNames = allWorkers() ?
        controller()->serviceProvider()->config()->allWorkers() :
        controller()->serviceProvider()->config()->workers();
    
    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation.

    size_t const maxRequestsPerWorker =
        controller()->serviceProvider()->config()->workerNumProcessingThreads();

    for (auto&& worker: workerNames) {
        _resultData.resultSets[worker] = list<SqlResultSet>();
        auto const requests = launchRequests(lock, worker, maxRequestsPerWorker);
        _requests.insert(_requests.cend(), requests.cbegin(), requests.cend());
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}


void SqlJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) stopRequest(lock, ptr);
    }
    _requests.clear();
}


void SqlJob::onRequestFinish(SqlRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  worker=" << request->worker());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    _numFinished++;

    // Update stats, including the result sets since they may carry
    // MySQL-specific errors reported by failed queries.
    _resultData.resultSets[request->worker()].push_back(request->responseData());

    // Try submitting a replacement request for the same worker. If none
    // would be launched then evaluate for the completion condition of the job.

    auto const requests = launchRequests(lock, request->worker());
    if (_requests.cend() == _requests.insert(_requests.cend(), requests.cbegin(), requests.cend())) {
        if (_requests.size() == _numFinished) {
            size_t numSuccess = 0;
            for (auto&& ptr: _requests) {
                if (ptr->extendedState() == Request::ExtendedState::SUCCESS) {
                    numSuccess++;
                }
            }
            finish(lock, numSuccess == _numFinished ? ExtendedState::SUCCESS :
                                                      ExtendedState::FAILED);
        }
    }
}


string SqlQueryJob::typeName() { return "SqlQueryJob"; }


SqlQueryJob::Ptr SqlQueryJob::create(string const& query,
                                     string const& user,
                                     string const& password,
                                     uint64_t maxRows,
                                     bool allWorkers,
                                     Controller::Ptr const& controller,
                                     string const& parentJobId,
                                     CallbackType const& onFinish,
                                     Job::Options const& options) {
    return Ptr(new SqlQueryJob(
        query,
        user,
        password,
        maxRows,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlQueryJob::SqlQueryJob(string const& query,
                         string const& user,
                         string const& password,
                         uint64_t maxRows,
                         bool allWorkers,
                         Controller::Ptr const& controller,
                         string const& parentJobId,
                         CallbackType const& onFinish,
                         Job::Options const& options)
    :   SqlJob(maxRows,
               allWorkers,
               controller,
               parentJobId,
               "SQL_QUERY",
               options),
        _query(query),
        _user(user),
        _password(password),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlQueryJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("query", query());
    result.emplace_back("user", user());
    result.emplace_back("max_rows", to_string(maxRows()));
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlQueryJob::launchRequests(util::Lock const& lock,
                                                  string const& worker,
                                                  size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlQueryJob>();
        requests.push_back(
            controller()->sqlQuery(
                worker,
                query(),
                user(),
                password(),
                maxRows(),
                [self] (SqlQueryRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlQueryJob::stopRequest(util::Lock const& lock,
                              SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlQueryRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlQueryJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlQueryJob>(lock, _onFinish);
}


string SqlCreateDbJob::typeName() { return "SqlCreateDbJob"; }


SqlCreateDbJob::Ptr SqlCreateDbJob::create(
        string const& database,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlCreateDbJob(
        database,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlCreateDbJob::SqlCreateDbJob(string const& database,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_CREATE_DATABASE",
               options),
        _database(database),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlCreateDbJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlCreateDbJob::launchRequests(util::Lock const& lock,
                                                     string const& worker,
                                                     size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlCreateDbJob>();
        requests.push_back(
            controller()->sqlCreateDb(
                worker,
                database(),
                [self] (SqlCreateDbRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlCreateDbJob::stopRequest(util::Lock const& lock,
                                 SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlCreateDbRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlCreateDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateDbJob>(lock, _onFinish);
}


string SqlDeleteDbJob::typeName() { return "SqlDeleteDbJob"; }


SqlDeleteDbJob::Ptr SqlDeleteDbJob::create(
        string const& database,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlDeleteDbJob(
        database,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlDeleteDbJob::SqlDeleteDbJob(string const& database,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_DROP_DATABASE",
               options),
        _database(database),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlDeleteDbJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlDeleteDbJob::launchRequests(util::Lock const& lock,
                                                     string const& worker,
                                                     size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlDeleteDbJob>();
        requests.push_back(
            controller()->sqlDeleteDb(
                worker,
                database(),
                [self] (SqlDeleteDbRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlDeleteDbJob::stopRequest(util::Lock const& lock,
                                 SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlDeleteDbRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlDeleteDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteDbJob>(lock, _onFinish);
}


string SqlEnableDbJob::typeName() { return "SqlEnableDbJob"; }


SqlEnableDbJob::Ptr SqlEnableDbJob::create(
        string const& database,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlEnableDbJob(
        database,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlEnableDbJob::SqlEnableDbJob(string const& database,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_ENABLE_DATABASE",
               options),
        _database(database),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlEnableDbJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlEnableDbJob::launchRequests(util::Lock const& lock,
                                                     string const& worker,
                                                     size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlEnableDbJob>();
        requests.push_back(
            controller()->sqlEnableDb(
                worker,
                database(),
                [self] (SqlEnableDbRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlEnableDbJob::stopRequest(util::Lock const& lock,
                                 SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlEnableDbRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlEnableDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlEnableDbJob>(lock, _onFinish);
}


string SqlDisableDbJob::typeName() { return "SqlDisableDbJob"; }


SqlDisableDbJob::Ptr SqlDisableDbJob::create(
        string const& database,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlDisableDbJob(
        database,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlDisableDbJob::SqlDisableDbJob(string const& database,
                                 bool allWorkers,
                                 Controller::Ptr const& controller,
                                 string const& parentJobId,
                                 CallbackType const& onFinish,
                                 Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_DISABLE_DATABASE",
               options),
        _database(database),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlDisableDbJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlDisableDbJob::launchRequests(util::Lock const& lock,
                                                      string const& worker,
                                                      size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlDisableDbJob>();
        requests.push_back(
            controller()->sqlDisableDb(
                worker,
                database(),
                [self] (SqlDisableDbRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlDisableDbJob::stopRequest(util::Lock const& lock,
                                  SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlDisableDbRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlDisableDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDisableDbJob>(lock, _onFinish);
}


string SqlGrantAccessJob::typeName() { return "SqlGrantAccessJob"; }


SqlGrantAccessJob::Ptr SqlGrantAccessJob::create(
        string const& database,
        string const& user,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlGrantAccessJob(
        database,
        user,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlGrantAccessJob::SqlGrantAccessJob(string const& database,
                                     string const& user,
                                     bool allWorkers,
                                     Controller::Ptr const& controller,
                                     string const& parentJobId,
                                     CallbackType const& onFinish,
                                     Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_GRANT_ACCESS",
               options),
        _database(database),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlGrantAccessJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlGrantAccessJob::launchRequests(util::Lock const& lock,
                                                        string const& worker,
                                                        size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlGrantAccessJob>();
        requests.push_back(
            controller()->sqlGrantAccess(
                worker,
                database(),
                user(),
                [self] (SqlGrantAccessRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlGrantAccessJob::stopRequest(util::Lock const& lock,
                                    SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlGrantAccessRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlGrantAccessJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlGrantAccessJob>(lock, _onFinish);
}


string SqlCreateTableJob::typeName() { return "SqlCreateTableJob"; }


SqlCreateTableJob::Ptr SqlCreateTableJob::create(
        string const& database,
        string const& table,
        string const& engine,
        string const& partitionByColumn,
        list<pair<string, string>> const& columns,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlCreateTableJob(
        database,
        table,
        engine,
        partitionByColumn,
        columns,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlCreateTableJob::SqlCreateTableJob(
        string const& database,
        string const& table,
        string const& engine,
        string const& partitionByColumn,
        list<pair<string, string>> const& columns,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_CREATE_TABLE",
               options),
        _database(database),
        _table(table),
        _engine(engine),
        _partitionByColumn(partitionByColumn),
        _columns(columns),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlCreateTableJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("engine", engine());
    result.emplace_back("partition_by_column", partitionByColumn());
    result.emplace_back("num_columns", to_string(columns().size()));
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlCreateTableJob::launchRequests(util::Lock const& lock,
                                                        string const& worker,
                                                        size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlCreateTableJob>();
        requests.push_back(
            controller()->sqlCreateTable(
                worker,
                database(),
                table(),
                engine(),
                partitionByColumn(),
                columns(),
                [self] (SqlCreateTableRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlCreateTableJob::stopRequest(util::Lock const& lock,
                                    SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlCreateTableRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlCreateTableJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateTableJob>(lock, _onFinish);
}


string SqlDeleteTableJob::typeName() { return "SqlDeleteTableJob"; }


SqlDeleteTableJob::Ptr SqlDeleteTableJob::create(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlDeleteTableJob(
        database,
        table,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlDeleteTableJob::SqlDeleteTableJob(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_DROP_TABLE",
               options),
        _database(database),
        _table(table),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlDeleteTableJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlDeleteTableJob::launchRequests(util::Lock const& lock,
                                                        string const& worker,
                                                        size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlDeleteTableJob>();
        requests.push_back(
            controller()->sqlDeleteTable(
                worker,
                database(),
                table(),
                [self] (SqlDeleteTableRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlDeleteTableJob::stopRequest(util::Lock const& lock,
                                    SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlDeleteTableRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlDeleteTableJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteTableJob>(lock, _onFinish);
}


string SqlRemoveTablePartitionsJob::typeName() { return "SqlRemoveTablePartitionsJob"; }


SqlRemoveTablePartitionsJob::Ptr SqlRemoveTablePartitionsJob::create(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlRemoveTablePartitionsJob(
        database,
        table,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlRemoveTablePartitionsJob::SqlRemoveTablePartitionsJob(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_REMOVE_TABLE_PARTITIONING",
               options),
        _database(database),
        _table(table),
        _onFinish(onFinish) {

    // Determine the type of the table
    auto const info = controller->serviceProvider()->config()->databaseInfo(database);
    if (find(info.partitionedTables.begin(),
             info.partitionedTables.end(), table) != info.partitionedTables.end()) {
        _isPartitioned = true;
        return;
    }

    // And the following test is just to ensure the table name is valid
    if (find(info.regularTables.begin(),
             info.regularTables.end(), table) != info.regularTables.end()) return;

    throw invalid_argument(
            context() + string(__func__) + "  unknown <database>.<table> '" + database +
            "'.'" + table + "'");
}


list<pair<string,string>> SqlRemoveTablePartitionsJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlRemoveTablePartitionsJob::launchRequests(util::Lock const& lock,
                                                                  string const& worker,
                                                                  size_t maxRequests) {
    list<SqlRequest::Ptr> requests;

    // Initialize worker's sub-collection if the first time seeing
    // this worker.
    if (not _workers2tables.count(worker)) {

        // This table must exist in both versions
        _workers2tables[worker].push_back(table());

        // Add chunk specific tables
        if (_isPartitioned) {

            // Locate all chunks registered on the worker. These chunks will be used
            // to build names of the corresponding chunk-specific partitioned tables.

            vector<ReplicaInfo> replicas;
            controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
                replicas,
                worker,
                database()
            );
            for (auto&& replica: replicas) {
                auto const chunk = replica.chunk();
                _workers2tables[worker].push_back(table() + "_" + to_string(chunk));
                _workers2tables[worker].push_back(table() + "FullOverlap_" + to_string(chunk));
            }
        }
    }

    // Launch up to (not to exceed) the specified number of requests for tables
    // by pulling table names from the worker's sub-collection. NOte that used
    // tables will get removed from the sub-collections.

    auto const self = shared_from_base<SqlRemoveTablePartitionsJob>();
    while (not _workers2tables[worker].empty() and requests.size() < maxRequests) {
        requests.push_back(
            controller()->sqlRemoveTablePartitions(
                worker,
                database(),
                _workers2tables[worker].front(),
                [self] (SqlRemoveTablePartitionsRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers2tables[worker].pop_front();
    }
    return requests;
}


void SqlRemoveTablePartitionsJob::stopRequest(util::Lock const& lock,
                                              SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlRemoveTablePartitionsRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlRemoveTablePartitionsJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlRemoveTablePartitionsJob>(lock, _onFinish);
}


string SqlDeleteTablePartitionJob::typeName() { return "SqlDeleteTablePartitionJob"; }


SqlDeleteTablePartitionJob::Ptr SqlDeleteTablePartitionJob::create(
        string const& database,
        string const& table,
        uint32_t transactionId,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlDeleteTablePartitionJob(
        database,
        table,
        transactionId,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlDeleteTablePartitionJob::SqlDeleteTablePartitionJob(
        string const& database,
        string const& table,
        uint32_t transactionId,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_DROP_TABLE_PARTITION",
               options),
        _database(database),
        _table(table),
        _transactionId(transactionId),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlDeleteTablePartitionJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlDeleteTablePartitionJob::launchRequests(util::Lock const& lock,
                                                                 string const& worker,
                                                                 size_t maxRequests) {

    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequests != 0) {
        auto const self = shared_from_base<SqlDeleteTablePartitionJob>();
        requests.push_back(
            controller()->sqlDeleteTablePartition(
                worker,
                database(),
                table(),
                transactionId(),
                [self] (SqlDeleteTablePartitionRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers.insert(worker);
    }
    return requests;
}


void SqlDeleteTablePartitionJob::stopRequest(util::Lock const& lock,
                                             SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlDeleteTablePartitionRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlDeleteTablePartitionJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteTablePartitionJob>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
