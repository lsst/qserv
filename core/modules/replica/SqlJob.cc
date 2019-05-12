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
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& SqlBaseJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        false,  /* exclusive */
        true    /* preemptable */
    };
    return options;
}


SqlBaseJob::SqlBaseJob(uint64_t maxRows,
                       bool allWorkers,
                       Controller::Ptr const& controller,
                       string const& parentJobId,
                       Job::Options const& options)
    :   Job(controller, parentJobId, "SQL", options),
        _maxRows(maxRows),
        _allWorkers(allWorkers) {
}


SqlJobResult const& SqlBaseJob::getResultData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(
            "SqlBaseJob::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<pair<string,string>> SqlBaseJob::persistentLogData() const {

    list<pair<string,string>> result;

    auto const& resultData = getResultData();

    // Per-worker stats

    for (auto&& itr: resultData.resultSets) {
        auto&& worker = itr.first;
        auto&& resultSet = itr.second;

        // ATTENTION: the 'error=' field is reported in the very end
        // of the string to simplify parsing of the string should
        // this be needed.

        result.emplace_back(
            "worker-stats",
            "worker=" + worker +
            " char_set_name=" + resultSet.charSetName +
            " has_result="    + string(   resultSet.hasResult ? "1" : "0") +
            " fields="        + to_string(resultSet.fields.size()) +
            " rows="          + to_string(resultSet.rows.size()) +
            " error="         +           resultSet.error
        );
    }
    return result;
}


void SqlBaseJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const workerNames = allWorkers() ?
        controller()->serviceProvider()->config()->allWorkers() :
        controller()->serviceProvider()->config()->workers();
    
    for (auto&& worker: workerNames) {
        _resultData.workers   [worker] = false;
        _resultData.resultSets[worker] = SqlResultSet();
        _requests.push_back(launchRequest(lock, worker));
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) finish(lock, ExtendedState::SUCCESS);
}


void SqlBaseJob::cancelImpl(util::Lock const& lock) {

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


void SqlBaseJob::onRequestFinish(SqlBaseRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  worker=" << request->worker());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Update stats, including the result sets since they may carry
    // MySQL-specific errors reported by failed queries.

    bool const requestSucceeded =
        request->extendedState() == Request::ExtendedState::SUCCESS;

    _resultData.workers   [request->worker()] = requestSucceeded;
    _resultData.resultSets[request->worker()] = request->responseData();

    // Evaluate the completion condition

    _numFinished++;
    if (requestSucceeded) _numSuccess++;

    if (_numFinished == _numLaunched) {
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
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
    :   SqlBaseJob(maxRows,
                   allWorkers,
                   controller,
                   parentJobId,
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

SqlBaseRequest::Ptr SqlQueryJob::launchRequest(util::Lock const& lock,
                                               string const& worker) {
    auto const self = shared_from_base<SqlQueryJob>();
    return controller()->sqlQuery(
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
    );
}


void SqlQueryJob::stopRequest(util::Lock const& lock,
                              SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlQuery(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(0 /* maxRows */,
                   allWorkers,
                   controller,
                   parentJobId,
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

SqlBaseRequest::Ptr SqlCreateDbJob::launchRequest(util::Lock const& lock,
                                                  string const& worker) {
    auto const self = shared_from_base<SqlCreateDbJob>();
    return controller()->sqlCreateDb(
        worker,
        database(),
        [self] (SqlCreateDbRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlCreateDbJob::stopRequest(util::Lock const& lock,
                                 SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlCreateDb(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(0 /* maxRows */,
                   allWorkers,
                   controller,
                   parentJobId,
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

SqlBaseRequest::Ptr SqlDeleteDbJob::launchRequest(util::Lock const& lock,
                                                  string const& worker) {
    auto const self = shared_from_base<SqlDeleteDbJob>();
    return controller()->sqlDeleteDb(
        worker,
        database(),
        [self] (SqlDeleteDbRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlDeleteDbJob::stopRequest(util::Lock const& lock,
                                 SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlDeleteDb(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(0 /* maxRows */,
                   allWorkers,
                   controller,
                   parentJobId,
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

SqlBaseRequest::Ptr SqlEnableDbJob::launchRequest(util::Lock const& lock,
                                                  string const& worker) {
    auto const self = shared_from_base<SqlEnableDbJob>();
    return controller()->sqlEnableDb(
        worker,
        database(),
        [self] (SqlEnableDbRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlEnableDbJob::stopRequest(util::Lock const& lock,
                                 SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlEnableDb(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(0 /* maxRows */,
                   allWorkers,
                   controller,
                   parentJobId,
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

SqlBaseRequest::Ptr SqlDisableDbJob::launchRequest(util::Lock const& lock,
                                                  string const& worker) {
    auto const self = shared_from_base<SqlDisableDbJob>();
    return controller()->sqlDisableDb(
        worker,
        database(),
        [self] (SqlDisableDbRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlDisableDbJob::stopRequest(util::Lock const& lock,
                                 SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlDisableDb(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlDisableDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDisableDbJob>(lock, _onFinish);
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
    :   SqlBaseJob(
            0,          /* maxRows */
            allWorkers,
            controller,
            parentJobId,
            options
        ),
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

SqlBaseRequest::Ptr SqlCreateTableJob::launchRequest(util::Lock const& lock,
                                                     string const& worker) {
    auto const self = shared_from_base<SqlCreateTableJob>();
    return controller()->sqlCreateTable(
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
    );
}


void SqlCreateTableJob::stopRequest(util::Lock const& lock,
                                    SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlCreateTable(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(
            0,          /* maxRows */
            allWorkers,
            controller,
            parentJobId,
            options
        ),
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

SqlBaseRequest::Ptr SqlDeleteTableJob::launchRequest(util::Lock const& lock,
                                                     string const& worker) {
    auto const self = shared_from_base<SqlDeleteTableJob>();
    return controller()->sqlDeleteTable(
        worker,
        database(),
        table(),
        [self] (SqlDeleteTableRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlDeleteTableJob::stopRequest(util::Lock const& lock,
                                    SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlDeleteTable(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
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
    :   SqlBaseJob(
            0,          /* maxRows */
            allWorkers,
            controller,
            parentJobId,
            options
        ),
        _database(database),
        _table(table),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlRemoveTablePartitionsJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}

SqlBaseRequest::Ptr SqlRemoveTablePartitionsJob::launchRequest(util::Lock const& lock,
                                                     string const& worker) {
    auto const self = shared_from_base<SqlRemoveTablePartitionsJob>();
    return controller()->sqlRemoveTablePartitions(
        worker,
        database(),
        table(),
        [self] (SqlRemoveTablePartitionsRequest::Ptr const& request) {
            self->onRequestFinish(request);
        },
        options(lock).priority,
        true,   /* keepTracking*/
        id()    /* jobId */
    );
}


void SqlRemoveTablePartitionsJob::stopRequest(util::Lock const& lock,
                                    SqlBaseRequest::Ptr const& request) {
    controller()->stopSqlRemoveTablePartitions(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlRemoveTablePartitionsJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlRemoveTablePartitionsJob>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
