// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
 /**
  * @file
  *
  * @brief A scheduler implementation that limits disk scans to one at
  * a time, but allows multiple queries to share I/O.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "wsched/ScanScheduler.h"

// System headers
#include <iostream>
#include <sstream>

// Third-party headers
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

// Local headers
#include "wcontrol/Foreman.h"
#include "wlog/WLogger.h"
#include "wsched/ChunkDisk.h"

namespace lsst {
namespace qserv {
namespace wsched {

ScanScheduler* dbgScanScheduler = 0; //< A symbol for gdb
ChunkDisk* dbgChunkDisk1 = 0; //< A symbol for gdb


////////////////////////////////////////////////////////////////////////
// class ScanScheduler
////////////////////////////////////////////////////////////////////////
ScanScheduler::ScanScheduler(wlog::WLogger::Ptr logger)
    : _maxRunning(32), // FIXME: set to some multiple of system proc count.
      _logger(logger) {
    _disks.push_back(boost::make_shared<ChunkDisk>(logger));
    dbgChunkDisk1 = _disks.front().get();
    dbgScanScheduler = this;
    assert(!_disks.empty());
}

bool
ScanScheduler::removeByHash(std::string const& hash) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    int numRemoved = _disks.front()->removeByHash(hash);
    // Consider creating poisoned list, that is checked during later
    // read/write ops; this would avoid O(nlogn) update for each
    // removal.
    return numRemoved > 0;
}

void
ScanScheduler::queueTaskAct(wbase::Task::Ptr incoming) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    _enqueueTask(incoming);
}

wbase::TaskQueuePtr
ScanScheduler::nopAct(wbase::TaskQueuePtr running) {
    if(!running) { throw std::invalid_argument("null run list"); }
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    int available = _maxRunning - running->size();
    return _getNextTasks(available);
}

/// @return a queue of all tasks ready to run.
///
wbase::TaskQueuePtr
ScanScheduler::newTaskAct(wbase::Task::Ptr incoming,
                          wbase::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    if(!running) { throw std::invalid_argument("null run list"); }

    _enqueueTask(incoming);
    // No free threads? Exit.
    // FIXME: Can do an I/O bound scan if there is memory and an idle
    // spindle.
    int available = _maxRunning - running->size();
    if(available <= 0) {
        return wbase::TaskQueuePtr();
    }
    return _getNextTasks(available);
}

wbase::TaskQueuePtr
ScanScheduler::taskFinishAct(wbase::Task::Ptr finished,
                             wbase::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());

    // No free threads? Exit.
    // FIXME: Can do an I/O bound scan if there is memory and an idle
    // spindle.
    std::ostringstream os;
    os << "Completed: " << "(" << finished->msg->chunkid()
       << ")" << finished->msg->fragment(0).query(0);
    _logger->debug(os.str());
    int available = _maxRunning - running->size();
    if(available <= 0) {
        return wbase::TaskQueuePtr();
    }
    return _getNextTasks(available);
}

void
ScanScheduler::markStarted(wbase::Task::Ptr t) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(!_disks.empty());
    _disks.front()->registerInflight(t);
}

void
ScanScheduler::markFinished(wbase::Task::Ptr t) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(!_disks.empty());
    _disks.front()->removeInflight(t);
}

/// @return true if data is okay.
bool
ScanScheduler::checkIntegrity() {
    boost::lock_guard<boost::mutex> guard(_mutex);
    return _integrityHelper();
}

/// @return true if data is okay
/// precondition: _mutex is locked.
bool
ScanScheduler::_integrityHelper() {
    ChunkDiskList::iterator i, e;
    for(i=_disks.begin(), e=_disks.end(); i != e; ++i) {
        if(!(**i).checkIntegrity()) return false;
    }
    return true;
}

/// Precondition: _mutex is already locked.
/// @return new tasks to run
/// TODO: preferential treatment for chunkId just run?
/// or chunkId that are currently running?
wbase::TaskQueuePtr
ScanScheduler::_getNextTasks(int max) {
    // FIXME: Select disk based on chunk location.
    assert(!_disks.empty());
    assert(_disks.front());
    std::ostringstream os;
    os << "_getNextTasks(" << max << ")>->->";
    _logger->debug(os.str());
    os.str("");
    wbase::TaskQueuePtr tq;
    ChunkDisk& disk = *_disks.front();

    // Check disks for candidate ones.
    // Pick one. Prefer a less-loaded disk: want to make use of i/o
    // from both disks. (for multi-disk support)
    bool allowNewChunk = (!disk.busy() && !disk.empty());
    while(max > 0) {
        wbase::Task::Ptr p = disk.getNext(allowNewChunk);
        if(!p) { break; }
        allowNewChunk = false; // Only allow one new chunk
        if(!tq) {
            tq.reset(new wbase::TaskQueue());
        }
        tq->push_back(p);

        os << "Making ready: " << *(tq->front());
        _logger->debug(os.str());
        os.str("");
        --max;
    }
    if(tq) {
        os << "Returning " << tq->size() << " to launch";
        _logger->debug(os.str());
    }
    assert(_integrityHelper());
    _logger->debug("_getNextTasks <<<<<");
    return tq;
}

/// Precondition: _mutex is locked.
void
ScanScheduler::_enqueueTask(wbase::Task::Ptr incoming) {
    if(!incoming) { throw std::invalid_argument("No task to enqueue"); }
    // FIXME: Select disk based on chunk location.
    assert(!_disks.empty());
    assert(_disks.front());
    _disks.front()->enqueue(incoming);
    std::ostringstream os;
    proto::TaskMsg const& msg = *(incoming->msg);
    os << "Adding new task: " << msg.chunkid()
       << " : " << msg.fragment(0).query(0);
    _logger->debug(os.str());
}

}}} // namespace lsst::qserv::wsched
