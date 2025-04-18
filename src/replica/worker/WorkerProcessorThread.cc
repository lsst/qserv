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
#include "replica/worker/WorkerProcessorThread.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/worker/WorkerProcessor.h"
#include "replica/worker/WorkerRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerProcessorThread");

}  // namespace

namespace lsst::qserv::replica {

WorkerProcessorThread::Ptr WorkerProcessorThread::create(WorkerProcessorPtr const& processor) {
    static unsigned int id = 0;
    return WorkerProcessorThread::Ptr(new WorkerProcessorThread(processor, id++));
}

WorkerProcessorThread::WorkerProcessorThread(WorkerProcessorPtr const& processor, unsigned int id)
        : _processor(processor), _id(id), _stop(false) {}

bool WorkerProcessorThread::isRunning() const { return _thread != nullptr; }

void WorkerProcessorThread::run() {
    if (isRunning()) return;

    _thread = make_unique<thread>([self = shared_from_this()]() {
        LOGS(_log, LOG_LVL_DEBUG, self->context() << "start");
        while (not self->_stop) {
            // Get the next request to process if any. This operation will block
            // until either the next request is available (returned a valid pointer)
            // or the specified timeout expires. In either case this thread has a chance
            // to re-evaluate the stopping condition.
            auto const request = self->_processor->_fetchNextForProcessing(self, 1000);
            if (self->_stop) {
                if (request) self->_processor->_processingRefused(request);
                continue;
            }
            if (request) {
                LOGS(_log, LOG_LVL_DEBUG,
                     self->context() << "begin processing"
                                     << "  id: " << request->id());
                bool finished = false;  // just to report the request completion
                try {
                    while (not(finished = request->execute())) {
                        if (self->_stop) {
                            LOGS(_log, LOG_LVL_DEBUG,
                                 self->context() << "rollback processing"
                                                 << "  id: " << request->id());
                            request->rollback();
                            self->_processor->_processingRefused(request);
                            break;
                        }
                    }
                } catch (WorkerRequestCancelled const& ex) {
                    LOGS(_log, LOG_LVL_DEBUG,
                         self->context() << "cancel processing"
                                         << "  id: " << request->id());
                    self->_processor->_processingFinished(request);
                }
                if (finished) {
                    LOGS(_log, LOG_LVL_DEBUG,
                         self->context() << "finish processing"
                                         << "  id: " << request->id()
                                         << "  status: " << WorkerRequest::status2string(request->status()));
                    self->_processor->_processingFinished(request);
                }
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, self->context() << "stop");

        self->_stopped();
    });
    _thread->detach();
}

void WorkerProcessorThread::stop() {
    if (not isRunning()) return;
    _stop = true;
}

void WorkerProcessorThread::_stopped() {
    _stop = false;
    _thread.reset(nullptr);
    _processor->_processorThreadStopped(shared_from_this());
}

}  // namespace lsst::qserv::replica
