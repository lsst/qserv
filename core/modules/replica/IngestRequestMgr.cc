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

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/IngestRequest.h"

using namespace std;
namespace {
string const context_ = "INGEST-REQUEST-MGR  ";
}
namespace lsst {
namespace qserv {
namespace replica {

IngestRequestMgr::Ptr IngestRequestMgr::create(ServiceProvider::Ptr const& serviceProvider,
                                               string const& workerName) {
    IngestRequestMgr::Ptr ptr(new IngestRequestMgr(serviceProvider, workerName));
    return ptr;
}


IngestRequestMgr::IngestRequestMgr(ServiceProvider::Ptr const& serviceProvider,
                                  string const& workerName)
        :   _serviceProvider(serviceProvider),
            _workerName(workerName) {
}


TransactionContribInfo IngestRequestMgr::find(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    auto const inputItr = find_if(
        _input.cbegin(),
        _input.cend(),
        [id](auto const& request) { return request->transactionContribInfo().id == id; }
    );
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
    throw IngestRequestNotFound(
            context_ + string(__func__) + " request " + to_string(id) + " was not found");
}


void IngestRequestMgr::submit(IngestRequest::Ptr const& request) {
    if (request == nullptr) {
        throw invalid_argument(
                context_ + string(__func__) + " null pointer passed into the method");
    }
    auto const contrib = request->transactionContribInfo();
    if ((contrib.status != TransactionContribInfo::Status::IN_PROGRESS) || (contrib.startTime != 0)) {
        throw logic_error(
                context_ + string(__func__) + " request " + to_string(contrib.id)
                + " has already been processed");
    }
    unique_lock<mutex> lock(_mtx);
    _input.push_front(request);
    lock.unlock();
    _cv.notify_one();
}


TransactionContribInfo IngestRequestMgr::cancel(unsigned int id) {
    unique_lock<mutex> lock(_mtx);
    auto const inputItr = find_if(
        _input.cbegin(),
        _input.cend(),
        [id](auto const& request) { return request->transactionContribInfo().id == id; }
    );
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
    throw IngestRequestNotFound(
            context_ + string(__func__) + " request " + to_string(id) + " was not found");

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
        throw IngestRequestNotFound(
                context_ + string(__func__) + " request " + to_string(id) + " was not found");
    }
    _output[id] = inProgressItr->second;
    _inProgress.erase(id);
}

}}} // namespace lsst::qserv::replica
