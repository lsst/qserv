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
#include "replica/IngestRequestMgr.h"

// System headers
#include <algorithm>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/IngestRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestRequestMgr");
string const context_ = "INGEST-REQUEST-MGR  ";
}  // namespace

namespace lsst { namespace qserv { namespace replica {

IngestRequestMgr::Ptr IngestRequestMgr::create(ServiceProvider::Ptr const& serviceProvider,
                                               string const& workerName) {
    IngestRequestMgr::Ptr ptr(new IngestRequestMgr(serviceProvider, workerName));

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

    // Contribution requests are sorted (ASC) by the creation time globally across
    // all transaction to ensure the eligible requests will be auto-resumed
    // in the original order.
    vector<TransactionContribInfo> contribsByCreateTimeASC;
    auto const transactions = databaseServices->transactions(TransactionInfo::State::STARTED);
    for (auto const& trans : transactions) {
        auto const contribs = databaseServices->transactionContribs(
                trans.id, TransactionContribInfo::Status::IN_PROGRESS, anyTable, workerName,
                TransactionContribInfo::TypeSelector::ASYNC);
        contribsByCreateTimeASC.insert(contribsByCreateTimeASC.end(), contribs.cbegin(), contribs.cend());
    }
    sort(contribsByCreateTimeASC.begin(), contribsByCreateTimeASC.end(),
         [](TransactionContribInfo const& a, TransactionContribInfo const& b) {
             return a.createTime < b.createTime;
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

    for (auto& contrib : contribsByCreateTimeASC) {
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
        // The auto-resume algorithm is evaluting a progress of requests from the newest
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
            databaseServices->loadedTransactionContrib(contrib, failed,
                                                       TransactionContribInfo::Status::LOAD_FAILED);
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
                databaseServices->readTransactionContrib(contrib, failed,
                                                         TransactionContribInfo::Status::READ_FAILED);
            }
        } else {
            // Opening the input source might get interupted by the restart.
            if (autoResume) {
                // Put the request into the input queue as if it's never been processed.
                ptr->submit(IngestRequest::resume(serviceProvider, workerName, contrib.id));
            } else {
                // Cancel at the starting phase
                contrib.error = errorStart;
                contrib.retryAllowed = true;
                databaseServices->startedTransactionContrib(contrib, failed,
                                                            TransactionContribInfo::Status::START_FAILED);
            }
        }
    }
    return ptr;
}

IngestRequestMgr::IngestRequestMgr(ServiceProvider::Ptr const& serviceProvider, string const& workerName)
        : _serviceProvider(serviceProvider), _workerName(workerName) {}

TransactionContribInfo IngestRequestMgr::find(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    auto const inputItr = find_if(_input.cbegin(), _input.cend(), [id](auto const& request) {
        return request->transactionContribInfo().id == id;
    });
    if (inputItr != _input.cend()) {
        return (*inputItr)->transactionContribInfo();
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
        return _serviceProvider->databaseServices()->transactionContrib(id);
    } catch (DatabaseServicesNotFound const& ex) {
        ;
    }
    throw IngestRequestNotFound(context_ + string(__func__) + " request " + to_string(id) + " was not found");
}

void IngestRequestMgr::submit(IngestRequest::Ptr const& request) {
    if (request == nullptr) {
        throw invalid_argument(context_ + string(__func__) + " null pointer passed into the method");
    }
    auto const contrib = request->transactionContribInfo();
    if ((contrib.status != TransactionContribInfo::Status::IN_PROGRESS) || (contrib.startTime != 0)) {
        throw logic_error(context_ + string(__func__) + " request " + to_string(contrib.id) +
                          " has already been processed");
    }
    unique_lock<mutex> lock(_mtx);
    _input.push_front(request);
    lock.unlock();
    _cv.notify_one();
}

TransactionContribInfo IngestRequestMgr::cancel(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    auto const inputItr = find_if(_input.cbegin(), _input.cend(), [id](auto const& request) {
        return request->transactionContribInfo().id == id;
    });
    if (inputItr != _input.cend()) {
        // Forced cancellation for requests that haven't been started.
        // This is the deterministic cancellation scenario as the request is
        // guaranteed to end up in the output queue with status 'CANCELLED'.
        auto const request = *inputItr;
        request->cancel();
        _input.erase(inputItr);
        _output[id] = request;
        return request->transactionContribInfo();
    }
    auto const inProgressItr = _inProgress.find(id);
    if (inProgressItr != _inProgress.cend()) {
        // Advisory cancellation by the processing thread when it will discover it
        // and if it won't be too late to cancel the request. Note that the thread
        // may be involved into the blocking disk, network or MySQL I/O call at this
        // time.
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

IngestRequest::Ptr IngestRequestMgr::next() {
    unique_lock<mutex> lock(_mtx);
    if (_input.empty()) {
        _cv.wait(lock, [&]() { return !_input.empty(); });
    }
    IngestRequest::Ptr const request = _input.back();
    _input.pop_back();
    _inProgress[request->transactionContribInfo().id] = request;
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
    _output[id] = inProgressItr->second;
    _inProgress.erase(id);
}

}}}  // namespace lsst::qserv::replica
