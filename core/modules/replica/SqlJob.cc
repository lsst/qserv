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

string SqlJob::typeName() { return "SqlJob"; }


Job::Options const& SqlJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        false,  /* exclusive */
        true    /* preemptable */
    };
    return options;
}


SqlJob::Ptr SqlJob::create(string const& query,
                           string const& user,
                           string const& password,
                           uint64_t maxRows,
                           bool allWorkers,
                           Controller::Ptr const& controller,
                           string const& parentJobId,
                           CallbackType const& onFinish,
                           Job::Options const& options) {
    return SqlJob::Ptr(
        new SqlJob(query,
                   user,
                   password,
                   maxRows,
                   allWorkers,
                   controller,
                   parentJobId,
                   onFinish,
                   options));
}


SqlJob::SqlJob(string const& query,
               string const& user,
               string const& password,
               uint64_t maxRows,
               bool allWorkers,
               Controller::Ptr const& controller,
               string const& parentJobId,
               CallbackType const& onFinish,
               Job::Options const& options)
    :   Job(controller, parentJobId, "SQL", options),
        _query     (query),
        _user      (user),
        _password  (password),
        _maxRows   (maxRows),
        _allWorkers(allWorkers),
        _onFinish  (onFinish) {
}


SqlJobResult const& SqlJob::getResultData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(
            "SqlJob::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<pair<string,string>> SqlJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("query",                 query());
    result.emplace_back("user",                  user());
    result.emplace_back("max_rows",    to_string(maxRows()));
    result.emplace_back("all_workers",    string(allWorkers() ? "1" : "0"));
    return result;
}


list<pair<string,string>> SqlJob::persistentLogData() const {

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


void SqlJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const self = shared_from_base<SqlJob>();

    auto const workerNames = allWorkers() ?
        controller()->serviceProvider()->config()->allWorkers() :
        controller()->serviceProvider()->config()->workers();
    
    for (auto&& worker : workerNames) {
        _resultData.workers   [worker] = false;
        _resultData.resultSets[worker] = SqlResultSet();
        _requests.push_back(
            controller()->sql(
                worker,
                query(),
                user(),
                password(),
                maxRows(),
                [self] (SqlRequest::Ptr request) {
                    self->_onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) setState(lock, State::FINISHED);
    else                  setState(lock, State::IN_PROGRESS);
}


void SqlJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr : _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            controller()->stopSql(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                id()        /* jobId */);
    }
    _requests.clear();
}


void SqlJob::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    notifyDefaultImpl<SqlJob>(lock, _onFinish);
}


void SqlJob::_onRequestFinish(SqlRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  worker=" << request->worker());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.
    
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

}}} // namespace lsst::qserv::replica
