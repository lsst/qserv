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
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
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
               Job::Options const& options,
               bool ignoreNonPartitioned)
    :   Job(controller, parentJobId, jobName, options),
        _maxRows(maxRows),
        _allWorkers(allWorkers),
        _ignoreNonPartitioned(ignoreNonPartitioned) {
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

    for (auto&& workerItr: resultData.resultSets) {
        auto&& worker = workerItr.first;
        auto&& workerResultSets = workerItr.second;
        string workerResultSetsStr;
        for (auto&& resultSet: workerResultSets) {
            for (auto&& resultSetItr: resultSet.queryResultSet) {
                auto&& context = resultSetItr.first;
                auto&& resultSet = resultSetItr.second;
                workerResultSetsStr +=
                    "(context=" + context +
                    ",extended_status=" + status2string(resultSet.extendedStatus) +
                    ",char_set_name=" + resultSet.charSetName +
                    ",has_result=" + string(resultSet.hasResult ? "1" : "0") +
                    ",fields=" + to_string(resultSet.fields.size()) +
                    ",rows=" + to_string(resultSet.rows.size()) +
                    ",error=" + resultSet.error +
                    "),";
            }
        }
        result.emplace_back(
            "worker-stats",
            "worker=" + worker + ",result-set=" + workerResultSetsStr
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
    auto itr = _requests.insert(_requests.cend(), requests.cbegin(), requests.cend());
    if (_requests.cend() == itr) {
        if (_requests.size() == _numFinished) {
            size_t numSuccess = 0;
            for (auto&& ptr: _requests) {
                if (ptr->extendedState() == Request::ExtendedState::SUCCESS) {
                    numSuccess++;
                } else {
                    // This too may also count as a success since the table might be processed
                    // before, when this job was ran.
                    if (ignoreNonPartitioned() and
                            ptr->extendedServerStatus() == ExtendedCompletionStatus::EXT_STATUS_MULTIPLE) {
                        auto&& responseData = request->responseData();
                        if (responseData.hasErrors() and responseData.allErrorsOf(
                                ExtendedCompletionStatus::EXT_STATUS_NOT_PARTITIONED_TABLE)) {
                            LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  id=" << request->id()
                                 << " [ignoreNonPartitioned & EXT_STATUS_NOT_PARTITIONED_TABLE]");
                            numSuccess++;
                        }
                    }
                }
            }
            finish(lock, numSuccess == _numFinished ? ExtendedState::SUCCESS :
                                                      ExtendedState::FAILED);
        }
    }
}


vector<vector<string>> SqlJob::distributeTables(vector<string> const& allTables,
                                                size_t numBins) {

    // If the total number of tables if less than the number of bins
    // then we won't be constructing empty bins.
    vector<vector<string>> tablesPerBin(min(numBins, allTables.size()));

    if (not tablesPerBin.empty()) {
        // The trivial 'round-robin' 
        for (size_t i=0; i<allTables.size(); ++i) {
            auto const bin = i % tablesPerBin.size();
            tablesPerBin[bin].push_back(allTables[i]);
        }
    }
    return tablesPerBin;
}


vector<string> SqlJob::workerTables(string const& worker,
                                    string const& database,
                                    string const& table) const {
    vector<string> tables;
    if (_isPartitioned(database, table)) {

        // The prototype table for creating chunks and chunk overlap tables
        tables.push_back(table);

        // Locate all chunks registered on the worker. These chunks will be used
        // to build names of the corresponding chunk-specific partitioned tables.
        vector<ReplicaInfo> replicas;
        controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
            replicas, worker, database);

        for (auto&& replica: replicas) {
            auto const chunk = replica.chunk();
            tables.push_back(table + "_" + to_string(chunk));
            tables.push_back(table + "FullOverlap_" + to_string(chunk));
        }
    } else {
        tables.push_back(table);
    }
    return tables;
}


bool SqlJob::_isPartitioned(string const& database,
                            string const& table) const {

    // Determine the type of the table
    auto const info = controller()->serviceProvider()->config()->databaseInfo(database);
    if (find(info.partitionedTables.begin(),
             info.partitionedTables.end(), table) != info.partitionedTables.end()) {
        return true;
    }

    // And the following test is just to ensure the table name is valid
    if (find(info.regularTables.begin(),
             info.regularTables.end(), table) != info.regularTables.end()) {
        return false;
    }
    throw invalid_argument(
            context() + string(__func__) + "  unknown <database>.<table> '" + database +
            "'.'" + table + "'");
}

}}} // namespace lsst::qserv::replica
