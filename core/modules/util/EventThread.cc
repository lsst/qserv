// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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
 *
 *  @author: John Gates,
 */

// Class header
#include "util/EventThread.h"

// System headers
#include <algorithm>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.EventThread");
}

namespace lsst {
namespace qserv {
namespace util {

/// Handle commands as they arrive until queEnd() is called.
void EventThread::handleCmds() {
    startup();
    while(_loop) {
        _cmd = _q->getCmd();
        _commandFinishCalled = false;
        _currentCommand = _cmd.get();
        if (_cmd != nullptr) {
            _q->commandStart(_cmd);
            specialActions(_cmd);
            _cmd->runAction(this);
            callCommandFinish(_cmd);
            // Reset _func in case it has a captured Command::Ptr,
            // which would keep it alive indefinitely.
            _cmd->resetFunc();
        }
        _cmd.reset();
        _currentCommand = nullptr;
    }
    finishup();
}


/// Ensure that commandFinish is only called once per loop.
void EventThread::callCommandFinish(Command::Ptr const& cmd) {
    if (_commandFinishCalled.exchange(true) == false) {
        _q->commandFinish(cmd);
    }
}


/// call this to start the thread
void EventThread::run() {
    std::thread t{&EventThread::handleCmds, this};
    _t = std::move(t);
}


EventThreadJoiner::EventThreadJoiner() {
    std::thread t(&EventThreadJoiner::joinLoop, this);
    _tJoiner = std::move(t);
}


EventThreadJoiner::~EventThreadJoiner() {
    if (_continue) {
        LOGS(_log, LOG_LVL_ERROR, "~EventThreadJoiner() called without shutdownJoiner() being called");
    }
}


void EventThreadJoiner::shutdownJoin() {
    _continue = false;
    LOGS(_log, LOG_LVL_DEBUG, "Waiting for joiner thread to finish.");
    _tJoiner.join();
}


void EventThreadJoiner::joinLoop() {
    EventThread::Ptr pet;
    while(true) {
        std::unique_lock<std::mutex> ulock(_mtxJoiner);
        if(!_eventThreads.empty()) {
            pet = _eventThreads.front();
            _eventThreads.pop();
            ulock.unlock();
            pet->join();
            pet.reset();
            _count--;
            LOGS(_log, LOG_LVL_DEBUG, "joined count=" << _count);
        } else {
            if (!_continue) break;
            ulock.unlock();
            std::this_thread::sleep_for(_sleepTime);
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "join loop exiting");
}


void EventThreadJoiner::addThread(EventThread::Ptr const& eventThread) {
    if (eventThread == nullptr) return;
    std::lock_guard<std::mutex> lg(_mtxJoiner);
    ++_count;
    _eventThreads.push(eventThread);
}

}}} // namespace lsst:qserv:util


