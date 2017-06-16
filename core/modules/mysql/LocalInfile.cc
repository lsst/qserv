// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "mysql/LocalInfile.h"

// System headers
#include <atomic>
#include <cassert>
#include <limits>
#include <mutex>
#include <string.h> // for memcpy

// Third-party headers
#include <mysql/mysql.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/LocalInfileError.h"
#include "mysql/RowBuffer.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.mysql.LocalInfile");

}



namespace lsst {
namespace qserv {
namespace mysql {

////////////////////////////////////////////////////////////////////////
// LocalInfile implementation
////////////////////////////////////////////////////////////////////////
int const infileBufferSize = 1024*1024; // 1M buffer

LocalInfile::LocalInfile(char const* filename, MYSQL_RES* result)
    : _filename(filename) {
    // Should have buffer >= sizeof(single row)
    const int defaultBuffer = infileBufferSize;
    _buffer = new char[defaultBuffer];
    _bufferSize = defaultBuffer;
    _leftover = 0;
    _leftoverSize = 0;
    assert(result);
    _rowBuffer = RowBuffer::newResRowBuffer(result);
}

LocalInfile::LocalInfile(char const* filename,
                         std::shared_ptr<RowBuffer> rowBuffer)
    : _filename(filename),
      _rowBuffer(rowBuffer) {
    // Should have buffer >= sizeof(single row)
    const int defaultBuffer = infileBufferSize;
    _buffer = new char[defaultBuffer];
    _bufferSize = defaultBuffer;
    _leftover = 0;
    _leftoverSize = 0;
    assert(_rowBuffer);
}

LocalInfile::~LocalInfile() {
    if (_buffer) {
        delete[] _buffer;
    }
}

int LocalInfile::read(char* buf, unsigned int bufLen) {
    assert(_rowBuffer);
    // Read into *buf
    unsigned copySize = bufLen;
    unsigned copied = 0;
    if (_leftoverSize) { // Try the leftovers first
        if (copySize > _leftoverSize) {
            copySize = _leftoverSize;
        }
        ::memcpy(buf, _leftover, copySize);
        copied = copySize;
        buf += copySize;
        bufLen -= copySize;
        _leftover += copySize;
        _leftoverSize -= copySize;
    }
    if (bufLen > 0) { // continue?
        // Leftover couldn't satisfy bufLen, so it's empty.
        // Re-fill the buffer.

        unsigned fetchSize = _rowBuffer->fetch(_buffer, _bufferSize);
        if (fetchSize == 0) {
            return copied;
        }
        if (fetchSize > bufLen) { // Fetched more than the buffer
            copySize = bufLen;
        } else {
            copySize = fetchSize;
        }
        ::memcpy(buf, _buffer, copySize);
        copied += copySize;
        _leftover = _buffer + copySize;
        _leftoverSize = fetchSize - copySize;
    }
    return copied;
}

int LocalInfile::getError(char* buf, unsigned int bufLen) {
    /// TODO: Improve error handling for LocalInfile.
    /// mysql docs indicate that this is called only when an init() or
    /// read() fails.
    char const initFailedMsg[] = "Failure initializing LocalInfile";
    if (!isValid()) {
        ::strncpy(buf, initFailedMsg, bufLen);
        return -1;
    }
    return 0;
}


void LocalInfile::Mgr::attach(MYSQL* mysql) {
    mysql_set_local_infile_handler(mysql,
                                   local_infile_init,
                                   local_infile_read,
                                   local_infile_end,
                                   local_infile_error,
                                   this);
}


void LocalInfile::Mgr::detachReset(MYSQL* mysql) {
    mysql_set_local_infile_default(mysql);
}


void LocalInfile::Mgr::prepareSrc(std::string const& filename, MYSQL_RES* result) {
    setBuffer(filename, RowBuffer::newResRowBuffer(result));
}


std::string LocalInfile::Mgr::prepareSrc(MYSQL_RES* result) {
    return insertBuffer(RowBuffer::newResRowBuffer(result));
}


std::string LocalInfile::Mgr::prepareSrc(std::shared_ptr<RowBuffer> const& rowBuffer, std::string const& qId) {
    LOGS(_log, LOG_LVL_DEBUG, qId << "rowBuffer=" << rowBuffer->dump());
    return insertBuffer(rowBuffer);
}


// mysql_local_infile_handler interface
int LocalInfile::Mgr::local_infile_init(void **ptr, const char *filename, void *userdata) {
    assert(userdata);
    //cout << "New infile:" << filename << "\n";
    LocalInfile::Mgr* m = static_cast<LocalInfile::Mgr*>(userdata);
    RowBuffer::Ptr rb= m->get(std::string(filename));
    assert(rb);
    LocalInfile* lf = new LocalInfile(filename, rb);
    *ptr = lf;
    if (!lf->isValid()) {
        return 1;
    }
    return 0;
    // userdata points at attached LocalInfileShared
}

int LocalInfile::Mgr::local_infile_read(void *ptr, char *buf, unsigned int buf_len) {
    return static_cast<LocalInfile*>(ptr)->read(buf, buf_len);
}

void LocalInfile::Mgr::local_infile_end(void *ptr) {
    LocalInfile* lf = static_cast<LocalInfile*>(ptr);
    delete lf;
}

int LocalInfile::Mgr::local_infile_error(void *ptr, char *error_msg, unsigned int error_msg_len) {
    return static_cast<LocalInfile*>(ptr)->getError(error_msg, error_msg_len);
}


std::string LocalInfile::Mgr::insertBuffer(std::shared_ptr<RowBuffer> const& rb) {
    std::string f = _nextFilename();
    _set(f, rb);
    return f;
}


void LocalInfile::Mgr::setBuffer(std::string const& s, std::shared_ptr<RowBuffer> const& rb) {
    bool newElem = _set(s, rb);
    if (!newElem) {
        throw LocalInfileError("Duplicate insertion in LocalInfile::Mgr");
    }
}


RowBuffer::Ptr LocalInfile::Mgr::get(std::string const& s) {
    std::lock_guard<std::mutex> lock(_mapMutex);
    RowBufferMap::iterator i = _map.find(s);
    if (i == _map.end()) { return std::shared_ptr<RowBuffer>(); }
    return i->second;
}


std::string LocalInfile::Mgr::_nextFilename() {
    static std::atomic<std::uint64_t> sequence(0);

    std::ostringstream os;
    os << "virtualinfile_" << ++sequence;
    return os.str();
}


bool LocalInfile::Mgr::_set(std::string const& s, std::shared_ptr<RowBuffer> const& rb) {
    std::lock_guard<std::mutex> lock(_mapMutex);
    auto res = _map.insert(std::pair<std::string, std::shared_ptr<RowBuffer>>(s, rb));
    return res.second;
}


}}} // namespace lsst::qserv::mysql
