/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/VerifyJob.h"

// System headers
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseServices.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<std::mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.VerifyJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////////////
///                ReplicaDiff                  ///
///////////////////////////////////////////////////

ReplicaDiff::ReplicaDiff()
    :   _notEqual(false) {
}

ReplicaDiff::ReplicaDiff(ReplicaInfo const& replica1,
                         ReplicaInfo const& replica2)
    :   _replica1(replica1),
        _replica2(replica2),
        _notEqual(false),
        _statusMismatch(false),
        _numFilesMismatch(false),
        _fileNamesMismatch(false),
        _fileSizeMismatch(false),
        _fileCsMismatch(false),
        _fileMtimeMismatch(false) {

    if ((replica1.database() != replica2.database()) or
        (replica1.chunk()    != replica2.chunk())) {
        throw std::invalid_argument(
                        "ReplicaDiff::ReplicaDiff(r1,r2)  incompatible aruments");
    }

    // Status and the number of files are expeted to match

    _statusMismatch   = replica1.status()          != replica2.status();
    _numFilesMismatch = replica1.fileInfo().size() != replica2.fileInfo().size();

    // Corresponding file entries must match

    std::map<std::string,ReplicaInfo::FileInfo> file2info1 = replica1.fileInfoMap();
    std::map<std::string,ReplicaInfo::FileInfo> file2info2 = replica2.fileInfoMap();

    for (auto&& f: file2info1) {

        // Check if each file is present in both collections
        std::string const& name = f.first;

        // The file name is required to be present in both replicas
        if (not file2info2.count(name)) {
            _fileNamesMismatch = true;
            continue;
        }

        ReplicaInfo::FileInfo const& file1 = file2info1[name];
        ReplicaInfo::FileInfo const& file2 = file2info2[name];

        _fileSizeMismatch = _fileSizeMismatch or (file1.size != file2.size);

        // Control sums are considered only if they're both defined
        _fileCsMismatch = _fileCsMismatch or
            ((not file1.cs.empty() and not file2.cs.empty()) and (file1.cs != file2.cs));

        _fileMtimeMismatch = _fileMtimeMismatch or (file1.mtime != file2.mtime);
    }
    _notEqual =
        _statusMismatch    or
        _numFilesMismatch  or
        _fileNamesMismatch or
        _fileSizeMismatch  or
        _fileCsMismatch    or
        _fileMtimeMismatch;
}

bool ReplicaDiff::isSelf() const {
    return _replica1.worker() == _replica2.worker();
}

std::string const& ReplicaDiff::flags2string() const {
    if (_flags.empty()) {
        if (_notEqual) {
            _flags = "DIFF ";
            if (_statusMismatch)    _flags += " status";
            if (_numFilesMismatch)  _flags += " files";
            if (_fileNamesMismatch) _flags += " name";
            if (_fileSizeMismatch)  _flags += " size";
            if (_fileCsMismatch)    _flags += " cs";
            if (_fileMtimeMismatch) _flags += " mtime";
        } else {
            _flags = "EQUAL";
        }
    }
    return _flags;
}

std::ostream& operator<<(std::ostream& os, ReplicaDiff const& ri) {

    ReplicaInfo const& r1 = ri.replica1();
    ReplicaInfo const& r2 = ri.replica2();

    os  << "ReplicaDiff\n"
        << "  <replica1>\n"
        << "    worker:   " << r1.worker()   << "\n"
        << "    database: " << r1.database() << "\n"
        << "    chunk:    " << r1.chunk()    << "\n"
        << "    status:   " << ReplicaInfo::status2string(r1.status()) << "\n"
        << "  <replica2>\n"
        << "    worker:   " << r2.worker()   << "\n"
        << "    database: " << r2.database() << "\n"
        << "    chunk:    " << r2.chunk()    << "\n"
        << "    status:   " << ReplicaInfo::status2string(r2.status()) << "\n"
        << "  notEqual:            " << (ri()                    ? "true" : "false") << "\n"
        << "    statusMismatch:    " << (ri.statusMismatch()     ? "true" : "false") << "\n"
        << "    numFilesMismatch:  " << (ri.numFilesMismatch()   ? "true" : "false") << "\n"
        << "    fileNamesMismatch: " << (ri.fileNamesMismatch()  ? "true" : "false") << "\n"
        << "    fileSizeMismatch:  " << (ri.fileSizeMismatch()   ? "true" : "false") << "\n"
        << "    fileCsMismatch:    " << (ri.fileCsMismatch()     ? "true" : "false") << "\n"
        << "    fileMtimeMismatch: " << (ri.fileMtimeMismatch()  ? "true" : "false") << "\n";
    return os;
}

/////////////////////////////////////////////////
///                VerifyJob                  ///
/////////////////////////////////////////////////

Job::Options const& VerifyJob::defaultOptions() {
    static Job::Options const options{
        0,      /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

VerifyJob::Ptr VerifyJob::create(
                        Controller::Ptr const& controller,
                        std::string const& parentJobId,
                        CallbackType onFinish,
                        CallbackTypeOnDiff onReplicaDifference,
                        size_t maxReplicas,
                        bool computeCheckSum,
                        Job::Options const& options) {
    return VerifyJob::Ptr(
        new VerifyJob(controller,
                      parentJobId,
                      onFinish,
                      onReplicaDifference,
                      maxReplicas,
                      computeCheckSum,
                      options));
}

VerifyJob::VerifyJob(Controller::Ptr const& controller,
                     std::string const& parentJobId,
                     CallbackType onFinish,
                     CallbackTypeOnDiff onReplicaDifference,
                     size_t maxReplicas,
                     bool computeCheckSum,
                     Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "VERIFY",
            options),
        _onFinish(onFinish),
        _onReplicaDifference(onReplicaDifference),
        _maxReplicas(maxReplicas),
        _computeCheckSum(computeCheckSum) {
}

void VerifyJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<VerifyJob>();

    // Launch the first batch of requests

    std::vector<ReplicaInfo> replicas;
    if (nextReplicas(replicas, _maxReplicas)) {
        for (ReplicaInfo const& replica: replicas) {
            auto request = _controller->findReplica(
                replica.worker(),
                replica.database(),
                replica.chunk(),
                [self] (FindRequest::Ptr request) {
                    self->onRequestFinish(request);
                },
                options().priority, /* inherited from the one of the current job */
                _computeCheckSum,
                true,               /* keepTracking*/
                _id                 /* jobId */
            );
            _replicas[request->id()] = replica;
            _requests[request->id()] = request;
        }
        setState(State::IN_PROGRESS);

    } else {
        // In theory this should never happen
        setState(State::FINISHED);
    }
}

void VerifyJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& entry: _requests) {
        auto const& request = entry.second;
        request->cancel();
        if (request->state() != Request::State::FINISHED) {
            _controller->stopReplicaFind(
                request->worker(),
                request->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
        }
    }
    _replicas.clear();
    _requests.clear();
}

void VerifyJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<VerifyJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void VerifyJob::onRequestFinish(FindRequest::Ptr request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker()
         << " chunk="  << request->chunk());

    LOCK(_mtx);

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) return;

    // The default version of the object won't have any difference
    // reported
    ReplicaDiff              selfReplicaDiff;   // against the previous state of the current replica
    std::vector<ReplicaDiff> otherReplicaDiff;  // against other known replicas

    auto self = shared_from_base<VerifyJob>();

    if (request->extendedState() == Request::ExtendedState::SUCCESS) {

        // TODO:
        // - check if the replica still exists. It's fine if it's gone
        //   because some jobs may chhose either to purge extra replicas
        //   or rebalance the cluster. So, no subscriber otification is needed
        //   here.

        ;

        // Compare new state of the replica against its older one which was
        // known to the database before this request was launched. Notify
        // a subscriber of any changes (after releasing LOCK(_mtx)).
        //
        // @see class ReplicaDiff for further specific details on replica
        // differece analysis.
        //
        // ATTENTIONS: Replica differeces are reported into the log stream only
        //             when no interest to be notified in the differences
        //             expressed by a caller (no callback provided).

        ReplicaInfo const& oldReplica = _replicas[request->id()];
        selfReplicaDiff = ReplicaDiff(oldReplica, request->responseData());
        if (selfReplicaDiff() and not _onReplicaDifference) {
            LOGS(_log, LOG_LVL_INFO, context() << "replica missmatch for self\n"
                 << selfReplicaDiff);
        }

        std::vector<ReplicaInfo> otherReplicas;
        _controller->serviceProvider()->databaseServices()->findReplicas(
                                                                otherReplicas,
                                                                oldReplica.chunk(),
                                                                oldReplica.database());
        for (auto&& replica: otherReplicas) {
            ReplicaDiff diff(request->responseData(), replica);
            if (not diff.isSelf()) {
                otherReplicaDiff.emplace_back(diff);
                if (diff() and not _onReplicaDifference)
                    LOGS(_log, LOG_LVL_INFO, context() << "replica missmatch for other\n"
                         << diff);
            }
        }

    } else {

        // Report the error and keep going
        LOGS(_log, LOG_LVL_ERROR, context() << "failed request " << request->context()
             << " worker: "   << request->worker()
             << " database: " << request->database()
             << " chunk: "    << request->chunk());
    }

    // Remove the processed replica, fetch another one and begin processing it

    _replicas.erase(request->id());
    _requests.erase(request->id());

    std::vector<ReplicaInfo> replicas;
    if (nextReplicas(replicas, 1)) {

        for (ReplicaInfo const& replica: replicas) {
            auto request = _controller->findReplica(
                replica.worker(),
                replica.database(),
                replica.chunk(),
                [self] (FindRequest::Ptr request) {
                    self->onRequestFinish(request);
                },
                options().priority, /* inherited from the one of the current job */
                _computeCheckSum,
                true,               /* keepTracking*/
                _id                 /* jobId */
            );
            _replicas[request->id()] = replica;
            _requests[request->id()] = request;
        }

    } else {

        // In theory this should never happen unless all replicas are gone
        // from the system or there was a problem to access the database.
        //
        // In any case check if no requests are in flight and finish if that's
        // the case.

        if (not _replicas.size()) {
            finish(ExtendedState::NONE);
        }
    }

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onReplicaDifference) {
        auto self = shared_from_base<VerifyJob>();
        std::async(
            std::launch::async,
            [self, selfReplicaDiff, otherReplicaDiff]() {
                self->_onReplicaDifference(self, selfReplicaDiff, otherReplicaDiff);
            }
        );
    }
    if (_state == State::FINISHED) notify();
}

bool VerifyJob::nextReplicas(std::vector<ReplicaInfo>& replicas,
                             size_t numReplicas) {

    return _controller->serviceProvider()->databaseServices()->findOldestReplicas(
                                                                    replicas,
                                                                    numReplicas);
}

}}} // namespace lsst::qserv::replica
