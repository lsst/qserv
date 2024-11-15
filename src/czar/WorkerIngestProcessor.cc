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
#include "czar/WorkerIngestProcessor.h"

using namespace std;

namespace lsst::qserv::czar::ingest {

Request::Request(function<Result()> const& processor, shared_ptr<ResultQueue> resultQueue)
        : _processor(processor), _resultQueue(resultQueue) {}

void Request::process() { _resultQueue->push(_processor()); }

shared_ptr<Processor> Processor::create(unsigned int numThreads) {
    return shared_ptr<Processor>(new Processor(numThreads));
}

void Processor::push(Request const& req) { _requestQueue->push(req); }

Processor::Processor(unsigned int numThreads) : _requestQueue(RequestQueue::create()) {
    for (unsigned int i = 0; i < numThreads; ++i) {
        _threads.push_back(thread([this]() {
            while (true) {
                _requestQueue->pop().process();
            }
        }));
    }
}

}  // namespace lsst::qserv::czar::ingest
