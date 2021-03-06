// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "xrdsvc/StreamBuffer.h"

// Third-party headers
#include "boost/utility.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.StreamBuffer");
}


namespace lsst {
namespace qserv {
namespace xrdsvc {

std::atomic<size_t> StreamBuffer::_totalBytes(0);

// Factory function, because this should be able to delete itself when Recycle() is called.
StreamBuffer::Ptr StreamBuffer::createWithMove(std::string &input) {
     Ptr ptr(new StreamBuffer(input));
     ptr->_selfKeepAlive = ptr;
     return ptr;
 }


StreamBuffer::StreamBuffer(std::string &input) {
    _dataStr = std::move(input);
    // TODO: try to make 'data' a const char* in xrootd code.
    // 'data' is not being changed after being passed, so hopefully not an issue.
    //_dataStr will not be used again, but this is ugly.
    data = (char*)(_dataStr.data());
    next = 0;

    _totalBytes += _dataStr.size();
    LOGS(_log, LOG_LVL_DEBUG, "StreamBuffer::_totalBytes=" << _totalBytes);
}


StreamBuffer::~StreamBuffer() {
    _totalBytes -= _dataStr.size();
    LOGS(_log, LOG_LVL_DEBUG, "~StreamBuffer::_totalBytes=" << _totalBytes);
}


/// xrdssi calls this to recycle the buffer when finished.
void StreamBuffer::Recycle() {
    {
        std::lock_guard<std::mutex> lg(_mtx);
        _doneWithThis = true;
    }
    _cv.notify_all();

    // delete this;
    // Effectively reset _selfKeepAlive, and if nobody else was
    // referencing this, this object will delete itself when
    // this function is done.
    // std::move is used instead of reset() as reset() could
    // result in _keepalive deleting itself while still in use.
    Ptr keepAlive = std::move(_selfKeepAlive);
}


void StreamBuffer::cancel() {
    // Recycle may still need to be called by XrdSsi or there will be a memory
    // leak. XrdSsi calling Recycle is beyond what can be controlled here, but
    // better a possible leak than corrupted memory or a permanently wedged
    // thread in a limited pool.
    // In any case, this code having an effect should be extremely rare.
    {
        std::lock_guard<std::mutex> lg(_mtx);
        _doneWithThis = true;
        _cancelled = true;
    }
    _cv.notify_all();
}


// Wait until recycle is called.
bool StreamBuffer::waitForDoneWithThis() {
    std::unique_lock<std::mutex> uLock(_mtx);
    _cv.wait(uLock, [this](){ return _doneWithThis == true; });
    return !_cancelled;
}

}}} // namespace lsst::qserv::xrdsvc
