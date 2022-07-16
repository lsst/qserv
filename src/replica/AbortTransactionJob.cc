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
#include "replica/AbortTransactionJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AbortTransactionJob");

}  // namespace

namespace lsst::qserv::replica {

string AbortTransactionJob::typeName() { return "AbortTransactionJob"; }

AbortTransactionJob::Ptr AbortTransactionJob::create(TransactionId transactionId, bool allWorkers,
                                                     Controller::Ptr const& controller,
                                                     string const& parentJobId, CallbackType const& onFinish,
                                                     int priority) {
    return AbortTransactionJob::Ptr(
            new AbortTransactionJob(transactionId, allWorkers, controller, parentJobId, onFinish, priority));
}

AbortTransactionJob::AbortTransactionJob(TransactionId transactionId, bool allWorkers,
                                         Controller::Ptr const& controller, string const& parentJobId,
                                         CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "ABORT_TRANSACTION", priority),
          _transactionId(transactionId),
          _allWorkers(allWorkers),
          _onFinish(onFinish) {}

Job::Progress AbortTransactionJob::progress() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    util::Lock lock(_mtx, context() + __func__);
    Progress jobProgress{0ULL, 0ULL};
    for (auto const& job : _jobs) {
        Progress const subJobProgress = job->progress();
        jobProgress.complete += subJobProgress.complete;
        jobProgress.total += subJobProgress.total;
    }
    return jobProgress;
}

SqlJobResult const& AbortTransactionJob::getResultData() const {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    if (state() == State::FINISHED) return _resultData;
    throw logic_error("AbortTransactionJob::" + string(__func__) +
                      "  the method can't be called until the job hasn't finished");
}

list<pair<string, string>> AbortTransactionJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<pair<string, string>> AbortTransactionJob::persistentLogData() const {
    list<pair<string, string>> result;
    _resultData.iterate([&result](SqlJobResult::Worker const& worker, SqlJobResult::Scope const& table,
                                  SqlResultSet::ResultSet const& resultSet) {
        result.emplace_back("status", "worker=" + worker + " table=" + table + " completed=" +
                                              bool2str(resultSet.extendedStatus == ProtocolStatusExt::NONE) +
                                              " error=" + resultSet.error);
    });
    return result;
}

void AbortTransactionJob::startImpl(util::Lock const& lock) {
    string const context_ = context() + string(__func__) + "  ";
    LOGS(_log, LOG_LVL_TRACE, context_);

    // Verify the current state of the transaction and the database

    auto const serviceProvider = controller()->serviceProvider();

    TransactionInfo transactionInfo;
    try {
        transactionInfo = serviceProvider->databaseServices()->transaction(_transactionId);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             context_ << "failed to located the transaction: " << _transactionId
                      << ", exception: " << ex.what());
        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }
    if (transactionInfo.state != TransactionInfo::State::IS_ABORTING) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "transaction " << transactionInfo.id << " is not IS_ABORTING");
        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }

    DatabaseInfo databaseInfo;
    try {
        databaseInfo = serviceProvider->config()->databaseInfo(transactionInfo.database);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             context_ << "failed to locate the database: '" << transactionInfo.database
                      << "', exception: " << ex.what());
        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }
    if (databaseInfo.isPublished) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "database " << databaseInfo.name << " is already PUBLISHED");
        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }

    // Submit a dedicated job for each unpublished table to process table instances on
    // a requested set of workers.
    auto self = shared_from_base<AbortTransactionJob>();
    for (auto&& tableName : databaseInfo.tables()) {
        // Skip tables that have been published.
        if (databaseInfo.findTable(tableName).isPublished) continue;
        auto job = SqlDeleteTablePartitionJob::create(
                _transactionId, tableName, _allWorkers, controller(), id(),
                bind(&AbortTransactionJob::_onChildJobFinish, self, _1), priority());
        job->start();
        _jobs.push_back(job);
    }
    if (_jobs.empty()) {
        finish(lock, ExtendedState::SUCCESS);
    }
}

void AbortTransactionJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    for (auto&& job : _jobs) {
        job->cancel();
    }
}

void AbortTransactionJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<AbortTransactionJob>(lock, _onFinish);
}

void AbortTransactionJob::_onChildJobFinish(SqlDeleteTablePartitionJob::Ptr const& job) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__ << "  table=" << job->table() << " id=" << job->id());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + string(__func__));

    if (state() == State::FINISHED) return;

    ++_numFinished;
    if (job->extendedState() == Job::SUCCESS) ++_numSuccess;

    // Harvest results from the job regardless of its completion status.
    // Jobs declared as failed might be still partially successful. In order
    // to determine which tables have not been processed one has to look
    // at the corresponding result set reported in the combined response data
    // object.
    _resultData.merge(job->getResultData());

    if (_numFinished == _jobs.size()) {
        finish(lock, _numSuccess == _numFinished ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}  // namespace lsst::qserv::replica
