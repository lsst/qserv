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
#include "replica/ingest/IngestRequestMgr.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/ingest/IngestRequest.h"
#include "replica/ingest/IngestResourceMgrP.h"
#include "replica/ingest/IngestResourceMgrT.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestRequestMgr");
string const context_ = "INGEST-REQUEST-MGR  ";
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<IngestRequestMgr> IngestRequestMgr::create(shared_ptr<ServiceProvider> const& serviceProvider,
                                                      string const& workerName) {
    shared_ptr<IngestRequestMgr> ptr(new IngestRequestMgr(serviceProvider, workerName));

    // ---------------------------------
    // -- RECOVERY AFTER RESTART MODE --
    // ---------------------------------

    // Process unfinished (queued or in-progress) contributions should any be
    // left before the service was shut down. The algorithms looks for
    // contributions in the open transactions.
    auto const databaseServices = serviceProvider->databaseServices();
    bool const cleanupOnResume =
            serviceProvider->config()->get<unsigned int>("worker", "async-loader-cleanup-on-resume") != 0;
    bool const autoResume =
            serviceProvider->config()->get<unsigned int>("worker", "async-loader-auto-resume") != 0;
    string const anyTable;

    // Contribution requests are sorted (DESC) by the creation time globally across
    // all transaction to ensure the eligible requests will be auto-resumed
    // in the original order.
    vector<TransactionContribInfo> contribsByCreateTimeDESC;
    auto const transactions = databaseServices->transactions(TransactionInfo::State::STARTED);
    for (auto const& trans : transactions) {
        auto const contribs = databaseServices->transactionContribs(
                trans.id, TransactionContribInfo::Status::IN_PROGRESS, anyTable, workerName,
                TransactionContribInfo::TypeSelector::ASYNC);
        contribsByCreateTimeDESC.insert(contribsByCreateTimeDESC.end(), contribs.cbegin(), contribs.cend());
    }
    sort(contribsByCreateTimeDESC.begin(), contribsByCreateTimeDESC.end(),
         [](TransactionContribInfo const& a, TransactionContribInfo const& b) {
             return a.createTime > b.createTime;
         });

    bool const failed = true;
    char const* errorStart =
            "The request was still in the queued state when the service was restarted."
            " Resuming requests at this stage after restart of the service was prohibited by"
            " an administrator of this Qserv instance in the configuration of the replication/Ingest system.";
    char const* errorReadData =
            "Reading input data was interrupted when the service was restarted."
            " Resuming requests at this stage after restart of the service was prohibited by"
            " an administrator of this Qserv instance in the configuration of the replication/Ingest system.";
    char const* errorLoadingIntoMySQL =
            "Loading into MySQL was interrupted when the service was restarted."
            " Resuming requests at this stage is not possible.";

    for (auto& contrib : contribsByCreateTimeDESC) {
        // Make the best effort to clean up the temporary files (if any) left after previous
        // run of the unfinished requests. Requests that are eligible to be resumed will
        // open new empty files as they will be being processed.
        if (cleanupOnResume && !contrib.tmpFile.empty()) {
            boost::system::error_code ec;
            fs::remove(contrib.tmpFile, ec);
            if (ec.value() != 0) {
                LOGS(_log, LOG_LVL_WARN,
                     context_ << "file removal failed for: '" << contrib.tmpFile << "', error: '"
                              << ec.message() << "', ec: " + to_string(ec.value()));
            }
        }

        // Note that an actual state of the in-progress contribution requests in which:
        //
        //   contrib.status == TransactionContribInfo::Status::IN_PROGRESS
        //
        // is determined by the series of the timestamps (from the oldest to the most recent):
        //
        //   contrib.createTime -- is guaranteed to be non-zero for any request recorded in
        //     the database. At this stage the request was sitting in the input queue waiting
        //     to be picked up by the next available processing thread. If the request was still
        //     in the queue then contrib.startTime, contrib.readTime and contrib.loadTime are
        //     guaranteed to be set to 0.
        //
        //   contrib.startTime -- is set to a non-zero value for contributions pulled from the input
        //     queue by a processing thread and put into the "in-progress" queue. The timestamp
        //     is set at a moment when the thread finishes creating a temporary file where
        //     the preprocessed content of the corresponding input file gets stored. Right after
        //     setting the timestamp the thread begins reading the input file. Timestamps
        //     contrib.readTime and contrib.loadTime are guaranteed to be set to 0 while the input
        //     file is still being read.
        //
        //   contrib.readTime -- is set to a non-zero value by a processing thread after finishing
        //     reading/preprocessing the input file and writing its preprocessed content into
        //     the temporary file. Right after that the thread begins loading the content of
        //     the file into MySQL. While the loading is still in progress the timestamp
        //     contrib.loadTime is guaranteed to be set to 0
        //
        //   contrib.loadTime -- is set to a non-zero value by a processing thread after finishing
        //     uploading the content of teh temporary file into MySQL. At this point the request
        //     is supposed to be completed and moved into the output queue.
        //
        // The auto-resume algorithm is evaluating a progress of requests from the newest
        // timestamps back to the oldest ones in order to determine at what stage each
        // request was before the restart.
        //
        // Requests that have a non-zero value in contrib.loadTime are not considered
        // by this  because a value of the requests's contrib.status will never be set
        // to TransactionContribInfo::Status::IN_PROGRESS.

        if (contrib.readTime != 0) {
            // Loading into MySQL may already began before the restart. It's no clear
            // at this point if it's succeeded or failed. Therefore the best strategy
            // here is to assume that it failed either right before restart or during
            // the restart. Hence we must cancel the contribution regardless of
            // the auto-resume policy.
            contrib.error = errorLoadingIntoMySQL;
            contrib.retryAllowed = false;
            databaseServices->loadedTransactionContrib(contrib, failed);
            continue;
        }
        if (contrib.startTime != 0) {
            // Reading from the input source might get interrupted by the restart.
            if (autoResume) {
                // Put the request into the input queue as if it's never been processed.
                contrib.startTime = 0;
                ptr->submit(IngestRequest::resume(serviceProvider, workerName, contrib.id));
            } else {
                // Cancel at reading the input data phase
                contrib.error = errorReadData;
                contrib.retryAllowed = true;
                databaseServices->readTransactionContrib(contrib, failed);
            }
        } else {
            // Opening the input source might get interrupted by the restart.
            if (autoResume) {
                // Put the request into the input queue as if it's never been processed.
                ptr->submit(IngestRequest::resume(serviceProvider, workerName, contrib.id));
            } else {
                // Cancel at the starting phase
                contrib.error = errorStart;
                contrib.retryAllowed = true;
                databaseServices->startedTransactionContrib(contrib, failed);
            }
        }
    }
    return ptr;
}

shared_ptr<IngestRequestMgr> IngestRequestMgr::test(shared_ptr<IngestResourceMgr> const& resourceMgr) {
    return shared_ptr<IngestRequestMgr>(new IngestRequestMgr(resourceMgr));
}

size_t IngestRequestMgr::inputQueueSize(string const& databaseName) const {
    unique_lock<mutex> lock(_mtx);
    if (databaseName.empty()) {
        size_t size = 0;
        for (auto&& itr : _input) {
            list<shared_ptr<IngestRequest>> const& queue = itr.second;
            size += queue.size();
        }
        return size;
    } else {
        auto const itr = _input.find(databaseName);
        if (itr == _input.cend()) return 0;
        return itr->second.size();
    }
}

size_t IngestRequestMgr::inProgressQueueSize(string const& databaseName) const {
    unique_lock<mutex> lock(_mtx);
    if (databaseName.empty()) return _inProgress.size();
    auto const itr = _concurrency.find(databaseName);
    if (itr == _concurrency.cend()) return 0;
    return itr->second;
}

size_t IngestRequestMgr::outputQueueSize() const {
    unique_lock<mutex> lock(_mtx);
    return _output.size();
}

IngestRequestMgr::IngestRequestMgr(shared_ptr<ServiceProvider> const& serviceProvider,
                                   string const& workerName)
        : _serviceProvider(serviceProvider),
          _workerName(workerName),
          _resourceMgr(IngestResourceMgrP::create(serviceProvider)) {}

TransactionContribInfo IngestRequestMgr::find(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    for (auto&& databaseItr : _input) {
        list<shared_ptr<IngestRequest>> const& queue = databaseItr.second;
        auto const inputItr = find_if(queue.cbegin(), queue.cend(), [id](auto const& request) {
            return request->transactionContribInfo().id == id;
        });
        if (inputItr != queue.cend()) {
            return (*inputItr)->transactionContribInfo();
        }
    }
    auto const inProgressItr = _inProgress.find(id);
    if (inProgressItr != _inProgress.cend()) {
        return inProgressItr->second->transactionContribInfo();
    }
    auto const outputItr = _output.find(id);
    if (outputItr != _output.cend()) {
        return outputItr->second->transactionContribInfo();
    }
    try {
        // This extra test is needed to allow unit testing the class w/o
        // making side effects.
        if (_serviceProvider != nullptr) {
            return _serviceProvider->databaseServices()->transactionContrib(id);
        }
    } catch (DatabaseServicesNotFound const& ex) {
        ;
    }
    throw IngestRequestNotFound(context_ + string(__func__) + " request " + to_string(id) + " was not found");
}

IngestRequestMgr::IngestRequestMgr(shared_ptr<IngestResourceMgr> const& resourceMgr)
        : _resourceMgr(resourceMgr == nullptr ? IngestResourceMgrT::create() : resourceMgr) {}

void IngestRequestMgr::submit(shared_ptr<IngestRequest> const& request) {
    if (request == nullptr) {
        throw invalid_argument(context_ + string(__func__) + " null pointer passed into the method");
    }
    auto const contrib = request->transactionContribInfo();
    if (contrib.database.empty() || contrib.createTime == 0) {
        throw invalid_argument(context_ + string(__func__) + " invalid request passed into the method");
    }
    if ((contrib.status != TransactionContribInfo::Status::IN_PROGRESS) || (contrib.startTime != 0)) {
        throw logic_error(context_ + string(__func__) + " request " + to_string(contrib.id) +
                          " has already been processed");
    }
    unique_lock<mutex> lock(_mtx);
    // Newest requests should go to the very end of the queue, so that
    // they will be processed after the older ones.
    _input[contrib.database].push_back(request);
    if (_updateMaxConcurrency(lock, contrib.database)) {
        // Concurrency has increased. Unblock all processing threads.
        lock.unlock();
        _cv.notify_all();
    } else {
        // Concurrency has not changed, or got lower. Unblock one processing thread for
        // the new request only.
        lock.unlock();
        _cv.notify_one();
    }
}

TransactionContribInfo IngestRequestMgr::cancel(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    // Scan input queues of all active databases.
    for (auto&& databaseItr : _input) {
        string const& databaseName = databaseItr.first;
        list<shared_ptr<IngestRequest>>& queue = databaseItr.second;
        auto const itr = find_if(queue.cbegin(), queue.cend(), [id](auto const& request) {
            return request->transactionContribInfo().id == id;
        });
        if (itr != queue.cend()) {
            // Forced cancellation for requests that haven't been started.
            // This is the deterministic cancellation scenario as the request is
            // guaranteed to end up in the output queue with status 'CANCELLED'.
            shared_ptr<IngestRequest> const request = *itr;
            request->cancel();
            queue.erase(itr);
            _output[id] = request;
            // Clear the queue and the dictionary if this was the very last element
            // in a scope of the database. Otherwise, refresh the concurrency limit
            // for the database in case if it was updated by the ingest workflow.
            if (queue.empty()) {
                _input.erase(databaseName);
                _maxConcurrency.erase(databaseName);
            } else {
                if (_updateMaxConcurrency(lock, databaseName)) {
                    // Concurrency has increased. Unblock all processing threads.
                    lock.unlock();
                    _cv.notify_all();
                }
            }
            return request->transactionContribInfo();
        }
    }
    auto const inProgressItr = _inProgress.find(id);
    if (inProgressItr != _inProgress.cend()) {
        // Advisory cancellation by the processing thread. It's going to be up
        // to the thread to discover the new state of the request, and cancel
        // the one if it won't be too late to do so. Note that the thread
        // may be involved into the blocking operations such as reading/writing
        // from disk, network, or interacting with MySQL at this time.
        inProgressItr->second->cancel();
        return inProgressItr->second->transactionContribInfo();
    }
    auto const outputItr = _output.find(id);
    if (outputItr != _output.cend()) {
        // No cancellation needed for contributions that have already been processed.
        // A client will receive the actual completion status of the request.
        return outputItr->second->transactionContribInfo();
    }
    throw IngestRequestNotFound(context_ + string(__func__) + " request " + to_string(id) + " was not found");
}

shared_ptr<IngestRequest> IngestRequestMgr::next() {
    unique_lock<mutex> lock(_mtx);
    shared_ptr<IngestRequest> request = _next(lock);
    if (request == nullptr) {
        _cv.wait(lock, [&]() {
            // The mutex is guaranteed to be re-locked here.
            request = _next(lock);
            return request != nullptr;
        });
    }
    return request;
}

shared_ptr<IngestRequest> IngestRequestMgr::next(chrono::milliseconds const& ivalMsec) {
    string const context = context_ + string(__func__) + " ";
    if (ivalMsec.count() == 0) throw invalid_argument(context + "the interval can not be 0.");
    unique_lock<mutex> lock(_mtx);
    shared_ptr<IngestRequest> request = _next(lock);
    if (request == nullptr) {
        bool const ivalExpired = !_cv.wait_for(lock, ivalMsec, [&]() {
            // The mutex is guaranteed to be re-locked here.
            request = _next(lock);
            return request != nullptr;
        });
        if (ivalExpired) {
            throw IngestRequestTimerExpired(context + "no request was found in the queue after waiting for " +
                                            to_string(ivalMsec.count()) + "ms");
        }
    }
    return request;
}

void IngestRequestMgr::completed(unsigned int id) {
    string const context = context_ + string(__func__) + " ";
    unique_lock<mutex> lock(_mtx);
    auto const inProgressItr = _inProgress.find(id);
    if (inProgressItr == _inProgress.cend()) {
        throw IngestRequestNotFound(context_ + string(__func__) + " request " + to_string(id) +
                                    " was not found");
    }
    shared_ptr<IngestRequest> const request = inProgressItr->second;
    _output[id] = request;
    _inProgress.erase(id);
    string const databaseName = request->transactionContribInfo().database;
    --(_concurrency[databaseName]);
    if (_concurrency[databaseName] == 0) _concurrency.erase(databaseName);
    // Refresh the concurrency limit for the database if there are outstanding
    // requests in the input queue.
    if (_input.count(databaseName) != 0) {
        if (_updateMaxConcurrency(lock, databaseName)) {
            // Concurrency has increased. Unblock all processing threads.
            lock.unlock();
            _cv.notify_all();
            return;
        }
    }
    lock.unlock();
    _cv.notify_one();
}

shared_ptr<IngestRequest> IngestRequestMgr::_next(unique_lock<mutex> const& lock) {
    shared_ptr<IngestRequest> request;
    for (auto&& databaseItr : _input) {
        string const& databaseName = databaseItr.first;
        list<shared_ptr<IngestRequest>> const& queue = databaseItr.second;
        // Skip empty queues and queues that have the current concurrency level
        // at or above the threshold. Note that the concurrency thresholds for
        // databases may change dynamically as directed by the ingest workflows.
        if (queue.empty()) continue;
        unsigned int const queueConcurrency = _maxConcurrency[databaseName];
        if ((queueConcurrency > 0) && (_concurrency[databaseName] >= queueConcurrency)) continue;
        // The (new) candidate request would be the very first hit, or the one
        // having the oldest create time.
        shared_ptr<IngestRequest> const& thisRequest = queue.front();
        bool const isCandidate = (request == nullptr) || (thisRequest->transactionContribInfo().createTime <
                                                          request->transactionContribInfo().createTime);
        if (isCandidate) request = thisRequest;
    }
    if (request != nullptr) {
        auto const contrib = request->transactionContribInfo();
        list<shared_ptr<IngestRequest>>& queue = _input[contrib.database];
        queue.pop_front();
        _inProgress[contrib.id] = request;
        ++(_concurrency[contrib.database]);
        // Clear the queue and the dictionary if this was the very last element
        // in a scope of the database.
        if (queue.empty()) {
            _input.erase(contrib.database);
            _maxConcurrency.erase(contrib.database);
        }
    }
    return request;
}

bool IngestRequestMgr::_updateMaxConcurrency(unique_lock<std::mutex> const& lock, string const& database) {
    bool concurrencyHasIncreased = false;
    // The previous concurrency limit will be initialize with 0, if the database
    // wasn't registered in the dictionary.
    unsigned int& maxConcurrencyRef = _maxConcurrency[database];
    unsigned int const newMaxConcurrency = _resourceMgr->asyncProcLimit(database);
    if (maxConcurrencyRef != newMaxConcurrency) {
        LOGS(_log, LOG_LVL_WARN,
             context_ << __func__ << " max.concurrency limit for database '" << database << "' changed from "
                      << maxConcurrencyRef << " to " << newMaxConcurrency << ".");
        concurrencyHasIncreased = (newMaxConcurrency == 0) ||
                                  ((maxConcurrencyRef != 0) && (newMaxConcurrency > maxConcurrencyRef));
        maxConcurrencyRef = newMaxConcurrency;
    }
    return concurrencyHasIncreased;
}

}  // namespace lsst::qserv::replica
