// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "mysql/LocalInfile.h"

// System headers
#include <cassert>
#include <limits>
#include <stdexcept>
#include <string.h> // for memcpy

// Third-party headers
#include <boost/thread.hpp>
#include <mysql/mysql.h>

// Qserv headers
#include "mysql/RowBuffer.h"

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
                         boost::shared_ptr<RowBuffer> rowBuffer)
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
    if(_buffer) {
        delete[] _buffer;
    }
}

int LocalInfile::read(char* buf, unsigned int bufLen) {
    assert(_rowBuffer);
    // Read into *buf
    unsigned copySize = bufLen;
    unsigned copied = 0;
    if(_leftoverSize) { // Try the leftovers first
        if(copySize > _leftoverSize) {
            copySize = _leftoverSize;
        }
        ::memcpy(buf, _leftover, copySize);
        copied = copySize;
        buf += copySize;
        bufLen -= copySize;
        _leftover += copySize;
        _leftoverSize -= copySize;
    }
    if(bufLen > 0) { // continue?
        // Leftover couldn't satisfy bufLen, so it's empty.
        // Re-fill the buffer.

        unsigned fetchSize = _rowBuffer->fetch(_buffer, _bufferSize);
        if(fetchSize == 0) {
            return copied;
        }
        if(fetchSize > bufLen) { // Fetched more than the buffer
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
    return 0;
}

////////////////////////////////////////////////////////////////////////
// LocalInfile::Mgr
////////////////////////////////////////////////////////////////////////
class LocalInfile::Mgr::Impl {
public:
    Impl() {}

    std::string insertBuffer(boost::shared_ptr<RowBuffer> rb) {
        std::string f = _nextFilename();
        _set(f, rb);
        return f;
    }

    void setBuffer(std::string const& s, boost::shared_ptr<RowBuffer> rb) {
        if(get(s)) {
            throw std::runtime_error("Duplicate insertion in LocalInfile::Mgr");
        }
        _set(s, rb);//RowBuffer::newResRowBuffer(result));
    }

    boost::shared_ptr<RowBuffer> get(std::string const& s) {
        boost::lock_guard<boost::mutex> lock(_mapMutex);
        RowBufferMap::iterator i = _map.find(s);
        if(i == _map.end()) { return boost::shared_ptr<RowBuffer>(); }
        return i->second;
    }

private:
    /// @return next filename
    std::string _nextFilename() {
        std::ostringstream os;
        // Switch to boost::atomic when boost 1.53 or c++11 (std::atomic)
        static int sequence = 0;
        static boost::mutex m;
        boost::lock_guard<boost::mutex> lock(m);

        os << "virtualinfile_" << ++sequence;
        return os.str();
    }

    void _set(std::string const& s, boost::shared_ptr<RowBuffer> rb) {
        boost::lock_guard<boost::mutex> lock(_mapMutex);
        _map[s] = rb;
    }

    typedef std::map<std::string, boost::shared_ptr<RowBuffer> > RowBufferMap;
    RowBufferMap _map;
    boost::mutex _mapMutex;
};


////////////////////////////////////////////////////////////////////////
// LocalInfile::Mgr
////////////////////////////////////////////////////////////////////////
LocalInfile::Mgr::Mgr()
    : _impl(new Impl) {
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
        _impl->setBuffer(filename, RowBuffer::newResRowBuffer(result));
}

std::string LocalInfile::Mgr::prepareSrc(MYSQL_RES* result) {
    return _impl->insertBuffer(RowBuffer::newResRowBuffer(result));
}

std::string LocalInfile::Mgr::prepareSrc(boost::shared_ptr<RowBuffer> rowBuffer) {
    return _impl->insertBuffer(rowBuffer);
}

// mysql_local_infile_handler interface

int LocalInfile::Mgr::local_infile_init(void **ptr, const char *filename, void *userdata) {
    assert(userdata);
    //cout << "New infile:" << filename << "\n";
    LocalInfile::Mgr* m = static_cast<LocalInfile::Mgr*>(userdata);
    boost::shared_ptr<RowBuffer> rb= m->_impl->get(std::string(filename));
    assert(rb);
    LocalInfile* lf = new LocalInfile(filename, rb);
    *ptr = lf;
    if(!lf->isValid()) {
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

int LocalInfile::Mgr::local_infile_error(void *ptr,
                                         char *error_msg,
                                         unsigned int error_msg_len) {
    return static_cast<LocalInfile*>(ptr)->getError(error_msg,
                                                    error_msg_len);
}

}}} // namespace lsst::qserv::mysql
