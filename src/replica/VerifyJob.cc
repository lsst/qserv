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
#include "replica/VerifyJob.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST header
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.VerifyJob");

}  // namespace

namespace lsst::qserv::replica {

///////////////////////////////////////////////////
///                ReplicaDiff                  ///
///////////////////////////////////////////////////

ReplicaDiff::ReplicaDiff()
        : _notEqual(false),
          _statusMismatch(false),
          _numFilesMismatch(false),
          _fileNamesMismatch(false),
          _fileSizeMismatch(false),
          _fileCsMismatch(false),
          _fileMtimeMismatch(false) {}

ReplicaDiff::ReplicaDiff(ReplicaInfo const& replica1, ReplicaInfo const& replica2)
        : _replica1(replica1),
          _replica2(replica2),
          _notEqual(false),
          _statusMismatch(false),
          _numFilesMismatch(false),
          _fileNamesMismatch(false),
          _fileSizeMismatch(false),
          _fileCsMismatch(false),
          _fileMtimeMismatch(false) {
    if ((replica1.database() != replica2.database()) or (replica1.chunk() != replica2.chunk())) {
        throw invalid_argument("ReplicaDiff::" + string(__func__) + "(r1,r2)  incompatible arguments");
    }

    // Status and the number of files are expeted to match

    _statusMismatch = replica1.status() != replica2.status();
    _numFilesMismatch = replica1.fileInfo().size() != replica2.fileInfo().size();

    // Corresponding file entries must match

    map<string, ReplicaInfo::FileInfo> file2info1 = replica1.fileInfoMap();
    map<string, ReplicaInfo::FileInfo> file2info2 = replica2.fileInfoMap();

    for (auto&& f : file2info1) {
        // Check if each file is present in both collections
        string const& name = f.first;

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
    _notEqual = _statusMismatch or _numFilesMismatch or _fileNamesMismatch or _fileSizeMismatch or
                _fileCsMismatch or _fileMtimeMismatch;
}

bool ReplicaDiff::isSelf() const { return _replica1.worker() == _replica2.worker(); }

string const& ReplicaDiff::flags2string() const {
    if (_flags.empty()) {
        if (_notEqual) {
            _flags = "DIFF ";
            if (_statusMismatch) _flags += " status";
            if (_numFilesMismatch) _flags += " files";
            if (_fileNamesMismatch) _flags += " name";
            if (_fileSizeMismatch) _flags += " size";
            if (_fileCsMismatch) _flags += " cs";
            if (_fileMtimeMismatch) _flags += " mtime";
        } else {
            _flags = "EQUAL";
        }
    }
    return _flags;
}

ostream& operator<<(ostream& os, ReplicaDiff const& ri) {
    ReplicaInfo const& r1 = ri.replica1();
    ReplicaInfo const& r2 = ri.replica2();

    os << "ReplicaDiff\n"
       << "  <replica1>\n"
       << "    worker:   " << r1.worker() << "\n"
       << "    database: " << r1.database() << "\n"
       << "    chunk:    " << r1.chunk() << "\n"
       << "    status:   " << ReplicaInfo::status2string(r1.status()) << "\n"
       << "  <replica2>\n"
       << "    worker:   " << r2.worker() << "\n"
       << "    database: " << r2.database() << "\n"
       << "    chunk:    " << r2.chunk() << "\n"
       << "    status:   " << ReplicaInfo::status2string(r2.status()) << "\n"
       << "  notEqual:            " << (ri() ? "true" : "false") << "\n"
       << "    statusMismatch:    " << (ri.statusMismatch() ? "true" : "false") << "\n"
       << "    numFilesMismatch:  " << (ri.numFilesMismatch() ? "true" : "false") << "\n"
       << "    fileNamesMismatch: " << (ri.fileNamesMismatch() ? "true" : "false") << "\n"
       << "    fileSizeMismatch:  " << (ri.fileSizeMismatch() ? "true" : "false") << "\n"
       << "    fileCsMismatch:    " << (ri.fileCsMismatch() ? "true" : "false") << "\n"
       << "    fileMtimeMismatch: " << (ri.fileMtimeMismatch() ? "true" : "false") << "\n";
    return os;
}

/////////////////////////////////////////////////
///                VerifyJob                  ///
/////////////////////////////////////////////////

string VerifyJob::typeName() { return "VerifyJob"; }

VerifyJob::Ptr VerifyJob::create(size_t maxReplicas, bool computeCheckSum,
                                 CallbackTypeOnDiff const& onReplicaDifference,
                                 Controller::Ptr const& controller, string const& parentJobId,
                                 CallbackType const& onFinish, int priority) {
    return VerifyJob::Ptr(new VerifyJob(maxReplicas, computeCheckSum, onReplicaDifference, controller,
                                        parentJobId, onFinish, priority));
}

VerifyJob::VerifyJob(size_t maxReplicas, bool computeCheckSum, CallbackTypeOnDiff const& onReplicaDifference,
                     Controller::Ptr const& controller, string const& parentJobId,
                     CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "VERIFY", priority),
          _maxReplicas(maxReplicas),
          _computeCheckSum(computeCheckSum),
          _onFinish(onFinish),
          _onReplicaDifference(onReplicaDifference) {
    if (0 == maxReplicas) {
        throw invalid_argument("VerifyJob::  parameter maxReplicas must be greater than 0");
    }
}

list<pair<string, string>> VerifyJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("max_replicas", to_string(maxReplicas()));
    result.emplace_back("compute_check_sum", bool2str(computeCheckSum()));
    return result;
}

void VerifyJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto self = shared_from_base<VerifyJob>();

    // Launch the first batch of requests

    vector<ReplicaInfo> replicas;
    _nextReplicas(lock, replicas, maxReplicas());

    if (replicas.empty()) {
        // In theory this should never happen unless the installation
        // doesn't have a single chunk.

        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  ** no replicas found in the database **");

        finish(lock, ExtendedState::FAILED);
        return;
    }
    for (ReplicaInfo const& replica : replicas) {
        auto request = controller()->findReplica(
                replica.worker(), replica.database(), replica.chunk(),
                [self](FindRequest::Ptr request) { self->_onRequestFinish(request); },
                priority(),              /* inherited from the one of the current job */
                computeCheckSum(), true, /* keepTracking*/
                id()                     /* jobId */
        );
        _replicas[request->id()] = replica;
        _requests[request->id()] = request;
    }
}

void VerifyJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& entry : _requests) {
        auto const& request = entry.second;
        request->cancel();
        if (request->state() != Request::State::FINISHED) {
            controller()->stopById<StopFindRequest>(request->worker(), request->id(), nullptr, /* onFinish */
                                                    priority(), true, /* keepTracking */
                                                    id() /* jobId */);
        }
    }
    _replicas.clear();
    _requests.clear();
}

void VerifyJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<VerifyJob>(lock, _onFinish);
}

void VerifyJob::_onRequestFinish(FindRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  database=" << request->database() << " worker=" << request->worker()
                   << " chunk=" << request->chunk());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // The default version of the object won't have any difference
    // reported
    ReplicaDiff selfReplicaDiff;           // against the previous state of the current replica
    vector<ReplicaDiff> otherReplicaDiff;  // against other known replicas

    auto self = shared_from_base<VerifyJob>();

    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        // TODO:
        // - check if the replica still exists. It's fine if it's gone
        //   because some jobs may choose either to purge extra replicas
        //   or re-balance the cluster. So, no subscriber notification is needed
        //   here.

        ;

        // Compare new state of the replica against its older one which was
        // known to the database before this request was launched. Notify
        // a subscriber of any changes (after releasing a lock on the mutex).
        //
        // @see class ReplicaDiff for further specific details on replica
        // difference analysis.
        //
        // ATTENTIONS: Replica differences are reported into the log stream only
        //             when no interest to be notified in the differences
        //             expressed by a caller (no callback provided).

        ReplicaInfo const& oldReplica = _replicas[request->id()];
        selfReplicaDiff = ReplicaDiff(oldReplica, request->responseData());
        if (selfReplicaDiff() and not _onReplicaDifference) {
            LOGS(_log, LOG_LVL_INFO, context() << "replica mismatch for self\n" << selfReplicaDiff);
        }

        vector<ReplicaInfo> otherReplicas;
        controller()->serviceProvider()->databaseServices()->findReplicas(otherReplicas, oldReplica.chunk(),
                                                                          oldReplica.database());

        for (auto&& replica : otherReplicas) {
            ReplicaDiff diff(request->responseData(), replica);
            if (not diff.isSelf()) {
                otherReplicaDiff.push_back(diff);
                if (diff() and not _onReplicaDifference)
                    LOGS(_log, LOG_LVL_INFO, context() << "replica mismatch for other\n" << diff);
            }
        }

    } else {
        // Report the error and keep going

        LOGS(_log, LOG_LVL_ERROR,
             context() << "failed request " << request->context() << " worker: " << request->worker()
                       << " database: " << request->database() << " chunk: " << request->chunk());
    }

    // Remove the processed replica, fetch another one and begin processing it

    _replicas.erase(request->id());
    _requests.erase(request->id());

    vector<ReplicaInfo> replicas;
    _nextReplicas(lock, replicas, 1);

    if (0 == replicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  ** no replicas found in the database **");

        // In theory this should never happen unless all replicas are gone
        // from the installation.

        finish(lock, ExtendedState::FAILED);
        return;
    }
    for (ReplicaInfo const& replica : replicas) {
        auto request = controller()->findReplica(
                replica.worker(), replica.database(), replica.chunk(),
                [self](FindRequest::Ptr request) { self->_onRequestFinish(request); },
                priority(),              /* inherited from the one of the current job */
                computeCheckSum(), true, /* keepTracking*/
                id()                     /* jobId */
        );
        _replicas[request->id()] = replica;
        _requests[request->id()] = request;
    }

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onReplicaDifference) {
        auto self = shared_from_base<VerifyJob>();
        thread notifier([self, selfReplicaDiff, otherReplicaDiff]() {
            self->_onReplicaDifference(self, selfReplicaDiff, otherReplicaDiff);
        });
        notifier.detach();
    }
}

void VerifyJob::_nextReplicas(util::Lock const& lock, vector<ReplicaInfo>& replicas, size_t numReplicas) {
    controller()->serviceProvider()->databaseServices()->findOldestReplicas(replicas, numReplicas);
}

}  // namespace lsst::qserv::replica
