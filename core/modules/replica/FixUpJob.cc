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
#include "replica/FixUpJob.h"

// System headers
#include <algorithm>
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ErrorReporting.h"
#include "replica/LockUtils.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FixUpJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& FixUpJob::defaultOptions() {
    static Job::Options const options{
        1,      /* priority */
        true,   /* exclusive */
        true    /* exclusive */
    };
    return options;
}

FixUpJob::Ptr FixUpJob::create(std::string const& databaseFamily,
                               Controller::Ptr const& controller,
                               std::string const& parentJobId,
                               CallbackType onFinish,
                               Job::Options const& options) {
    return FixUpJob::Ptr(
        new FixUpJob(databaseFamily,
                     controller,
                     parentJobId,
                     onFinish,
                     options));
}

FixUpJob::FixUpJob(std::string const& databaseFamily,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType onFinish,
                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "FIXUP",
            options),
        _databaseFamily(databaseFamily),
        _onFinish(onFinish),
        _numIterations(0),
        _numFailedLocks(0),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

FixUpJob::~FixUpJob() {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider()->chunkLocker().release(_id);
}

FixUpJobResult const& FixUpJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "FixUpJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string FixUpJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily());
}

void FixUpJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ASSERT_LOCK(_mtx, context() + "startImpl");

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<FixUpJob>();

    _findAllJob = FindAllJob::create(
        _databaseFamily,
        _controller,
        _id,
        [self] (FindAllJob::Ptr job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void FixUpJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    ASSERT_LOCK(_mtx, context() + "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplication(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }

    _chunk2requests.clear();
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}


void FixUpJob::restart() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    ASSERT_LOCK(_mtx, context() + "restart");

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw std::logic_error("FixUpJob::restart ()  not allowed in this object state");
    }
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void FixUpJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<FixUpJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void FixUpJob::onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.
    
    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "onPrecursorJobFinish");

    if (_state == State::FINISHED) return;

    /////////////////////////////////////////////////////////////////
    // Proceed with the replication effort only if the precursor job
    // has succeeded.

    if (_findAllJob->extendedState() == ExtendedState::SUCCESS) {

        /////////////////////////////////////////////////////////////////
        // Analyse results and prepare a replication plan to fix chunk
        // co-location for under-represented chunks

        FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

        auto self = shared_from_base<FixUpJob>();

        for (auto&& chunk2workers: replicaData.isColocated) {
            unsigned int chunk = chunk2workers.first;

            for (auto&& worker2colocated: chunk2workers.second) {
                std::string const& destinationWorker = worker2colocated.first;
                bool        const  isColocated       = worker2colocated.second;

                if (isColocated) continue;

                // Chunk locking is mandatory. If it's not possible to do this now then
                // the job will need to make another attempt later.

                if (not _controller->serviceProvider()->chunkLocker().lock({_databaseFamily, chunk}, _id)) {
                    ++_numFailedLocks;
                    continue;
                }

                // Iterate over all participating databases, find the ones which aren't
                // represented on the worker, find a suitable source worker which has
                // a complete chunk for the database and which (the worker) is not the same
                // as the current one and submite the replication request.

                for (auto&& database: replicaData.databases.at(chunk)) {

                    if (not replicaData.chunks.chunk(chunk)
                                              .database(database)
                                              .workerExists(destinationWorker)) {

                        // Finding a source worker first
                        std::string sourceWorker;
                        for (auto&& worker: replicaData.complete.at(chunk).at(database)) {
                            if (worker != destinationWorker) {
                                sourceWorker = worker;
                                break;
                            }
                        }
                        if (sourceWorker.empty()) {
                            LOGS(_log, LOG_LVL_ERROR, context()
                                 << "onPrecursorJobFinish  failed to find a source worker for chunk: "
                                 << chunk << " and database: " << database);
                            release(chunk);
                            finish(ExtendedState::FAILED);
                            break;
                        }

                        // Finally, launch the replication request and register it for further
                        // tracking (or cancellation, should the one be requested)

                        ReplicationRequest::Ptr ptr =
                            _controller->replicate(
                                destinationWorker,
                                sourceWorker,
                                database,
                                chunk,
                                [self] (ReplicationRequest::Ptr ptr) {
                                    self->onRequestFinish(ptr);
                                },
                                0,      /* priority */
                                true,   /* keepTracking */
                                true,   /* allowDuplicate */
                                _id     /* jobId */
                            );

                        _chunk2requests[chunk][destinationWorker][database] = ptr;
                        _requests.push_back(ptr);
                        _numLaunched++;
                    }
                }
                if (_state == State::FINISHED) break;
            }
            if (_state == State::FINISHED) break;
        }
        if (_state != State::FINISHED) {

            // ATTENTION: We need to evaluate reasons why no single request was
            // launched while the job is still in the unfinished state and take
            // proper actions. Otherwise (if this isn't done here) the object will
            // get into a "zombie" state.
            if (not _requests.size()) {

                // Finish right away if no problematic chunks found
                if (not _numFailedLocks) {
                    finish(ExtendedState::SUCCESS);
                } else {

                    // Some of the chuks were locked and yet, no sigle request was
                    // lunched. Hence we should start another iteration by requesting
                    // the fresh state of the chunks within the family.
                    restart();
                }
            }
        }

    } else {
        finish(ExtendedState::FAILED);
    }
    if (_state == State::FINISHED) notify();
}

void FixUpJob::onRequestFinish(ReplicationRequest::Ptr const& request) {

    std::string  const database = request->database();
    std::string  const worker   = request->worker();
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish"
         << "  database=" << database
         << "  worker="   << worker
         << "  chunk="    << chunk);

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED)  {
        release(chunk);
        return;
    }

    LOCK(_mtx, context() + "onRequestFinish");

    if (_state == State::FINISHED) {
        release(chunk);
        return;
    }

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _numSuccess++;
        _replicaData.replicas.push_back(request->responseData());
        _replicaData.chunks[chunk][database][worker] = request->responseData();
        _replicaData.workers[worker] = true;
    } else {
        _replicaData.workers[worker] = false;
    }

    // Make sure the chunk is released if this was the last
    // request in its scope.

    _chunk2requests.at(chunk).at(worker).erase(database);
    if (_chunk2requests.at(chunk).at(worker).empty()) {
        _chunk2requests.at(chunk).erase(worker);
        if (_chunk2requests.at(chunk).empty()) {
            _chunk2requests.erase(chunk);
            release(chunk);
        }
    }

    // Evaluate the status of on-going operations to see if the job
    // has finished.

    if (_numFinished == _numLaunched) {
        if (_numSuccess == _numLaunched) {
            if (_numFailedLocks) {
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.
                restart();
                return;
            } else {
                finish(ExtendedState::SUCCESS);
            }
        } else {
            finish(ExtendedState::FAILED);
        }
    }
    if (_state == State::FINISHED) notify();
}

void FixUpJob::release(unsigned int chunk) {

    // THREAD-SAFETY NOTE: This method is thread-agnostic because it's trading
    // a static context of the request with an external service which is guaranteed
    // to be thread-safe.

    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);

    Chunk chunkObj {_databaseFamily, chunk};
    _controller->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
