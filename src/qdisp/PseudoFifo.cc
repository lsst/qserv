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
#include "qdisp/PseudoFifo.h"

// System headers

// Qserv headers
#include "global/LogContext.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdsip.PseudoFifo");
}

namespace lsst {
namespace qserv {
namespace qdisp {

uint32_t PseudoFifo::Element::seq = 0;


void PseudoFifo::Element::wait() {
    LOGS(_log, LOG_LVL_INFO, "&&& wait a");
    std::unique_lock<std::mutex> eLock(_eMtx);
    _eCv.wait(eLock, [this](){ return _go; });
    LOGS(_log, LOG_LVL_INFO, "&&& wait b");
}


void PseudoFifo::Element::go() {
    {
        std::unique_lock<std::mutex> eLock(_eMtx);
        _go = true;
    }
    _eCv.notify_one();
}


PseudoFifo::Element::Ptr PseudoFifo::queueAndWait() {
    Element::Ptr thisElem = std::make_shared<Element>(*this);
    {
        std::unique_lock<std::mutex> qLock(_qMtx);
        _queue.push_back(thisElem);
        _runSomeElements();
    }
    thisElem->wait(); // wait until _runSomeElements pops this from the queue.
    return thisElem;
}


void PseudoFifo::_runSomeElements() {
    LOGS(_log, LOG_LVL_INFO, "&&& a _runningCount=" << _runningCount << " max=" << _maxRunningCount);
    while (_runningCount < _maxRunningCount && !_queue.empty()) {
        Element::Ptr qElem = _queue.front();
        _queue.pop_front();
        ++_runningCount;
        LOGS(_log, LOG_LVL_INFO, "&&& b _runningCount=" << _runningCount);
        qElem->go();
    }
}


}}} // namespace lsst::qserv::qdisp
