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
// XrdBufferSource: a fragment-iterated interface to a local file or an
// xrootd file descriptor. Facilitates transferring bytes directly
// from the xrootd realm to a fragment consumer (probably the table
// merger). Allowing both types input sources makes it easier to
// reduce buffering and disk usage, theoretically improving overall
// latency.

#include "xrdc/XrdBufferSource.h"

// System headers
#include <cassert>
#include <errno.h>
#include <fcntl.h>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "xrdc/xrdfile.h"


namespace lsst {
namespace qserv {
namespace xrdc {

////////////////////////////////////////////////////////////////////////
// XrdBufferSource public
////////////////////////////////////////////////////////////////////////
XrdBufferSource::XrdBufferSource(int xrdFd, int fragmentSize)
    : _xrdFd(xrdFd),
      _buffer(0),
      _fragSize(fragmentSize),
      _stop(false) {
    _setup(false);
}

XrdBufferSource::XrdBufferSource(std::string const& fileName, int fragmentSize,
                                 bool debug)
    : _xrdFd(0),
      _fileName(fileName),
      _buffer(0),
      _fragSize(fragmentSize),
      _stop(false) {
    _setup(debug);
}

XrdBufferSource::~XrdBufferSource() {
    if(_buffer != NULL) free(_buffer);
    if(_xrdFd != 0) {
        xrdClose(_xrdFd);
    } else if(_realFd != 0) {
        ::close(_realFd);
        _realFd = 0;
    }
}

////////////////////////////////////////////////////////////////////////
// XrdBufferSource public
////////////////////////////////////////////////////////////////////////
util::PacketBuffer::Value XrdBufferSource::getFirstValue() {
    return Value(_buffer, _occupiedSize);
}

void XrdBufferSource::increment(util::PacketBuffer& p) {
    int newSize = _fragSize;
    _fill(_buffer, newSize);
    _occupiedSize = newSize;
    setCurrent(p, _buffer, _occupiedSize);
}

bool XrdBufferSource::incrementExtend(util::PacketBuffer& p) {
    LOGF_DEBUG("XrdBufferSource Realloc to %1%" %
               (_occupiedSize + _fragSize));
    void* ptr = ::realloc(_buffer, _occupiedSize + _fragSize);
    if(!ptr) {
        errno = ENOMEM;
        throw "Failed to realloc for XrdBufferSource.";
    }
    _buffer = static_cast<char*>(ptr);
    int fillSize = _fragSize;
    char* newFill = _buffer + _occupiedSize;
    _fill(newFill, fillSize);
    if(fillSize == 0) {
        return false;
    } else {
        _occupiedSize += fillSize;
        setCurrent(p, _buffer, _occupiedSize);
        return true;
    }
}

////////////////////////////////////////////////////////////////////////
// XrdBufferSource private methods
////////////////////////////////////////////////////////////////////////
void XrdBufferSource::_setup(bool debug) {
    _errno = 0; // Important to initialize for proper error handling.
    const int minFragment = 65536;
    _memo = false;
    if(!debug && (_fragSize < minFragment)) _fragSize = minFragment;

    assert(sizeof(char) == 1);
    assert(_buffer == 0);
    assert(_fragSize > 0);
    // malloc() is used here rather than the "new" operator because a low-level
    // bucket of bytes is desired, and so we can use realloc
    _buffer = static_cast<char*>(::malloc(_fragSize));
    if(_buffer == NULL) {
        errno = ENOMEM;
        throw "Failed to malloc for XrdBufferSource.";
    }
    if(!_fileName.empty()) {
        _realFd = open(_fileName.c_str(), O_RDONLY);
        if(_realFd < 0) {
            throw "couldn't open file in XrdBufferSource";
        }
    }
    int newSize = _fragSize;
    _fill(_buffer, newSize);
    _occupiedSize = newSize;
}

void XrdBufferSource::_fill(char*& buf, int& len) {
    int readRes = 0;
    if(_stop) {
        buf = 0;
        len = 0;
        return;
    }
    if(_xrdFd != 0) {
        readRes = xrdRead(_xrdFd, buf, static_cast<unsigned long long>(len));
        if (readRes < 0) {
            throw "Remote I/O error during XRD read.";
        }
    } else if(!_fileName.empty()) {
        readRes = ::read(_realFd, buf, len);
    } else {
        readRes = 0;
    }

    if(readRes < 0) {
        //Report error somehow
        _errno = errno;
    }
    if(readRes < len) {
        _stop = true;
    }
    len = readRes;
}

}}} // namespace lsst::qserv::xrdc
