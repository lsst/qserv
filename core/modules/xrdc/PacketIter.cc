/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
// PacketIter: a fragment-iterated interface to a local file or an
// xrootd file descriptor. Facilitates transferring bytes directly
// from the xrootd realm to a fragment consumer (probably the table
// merger). Allowing both types input sources makes it easier to
// reduce buffering and disk usage, theoretically improving overall
// latency.

#include "xrdc/PacketIter.h"
#include "xrdc/xrdfile.h"
#include "log/Logger.h"

#include <fcntl.h>
#include <errno.h>
#include <iostream>

namespace lsst {
namespace qserv {
namespace xrdc {

    
PacketIter::PacketIter()
  : _xrdFd(-1), _current(0,0), _stop(false)
{}

PacketIter::PacketIter(int xrdFd, int fragmentSize)
    : _xrdFd(xrdFd),
      _fragSize(fragmentSize),
      _current(0,0),
      _stop(false) {
    _setup(false);
}

PacketIter::PacketIter(std::string const& fileName, int fragmentSize,
                       bool debug)
    : _xrdFd(0),
      _fileName(fileName),
      _fragSize(fragmentSize),
      _current(0,0),
      _stop(false) {
    _setup(debug);
}

PacketIter::~PacketIter() {
    if(_buffer != NULL) free(_buffer);
    if(_xrdFd != 0) {
        xrdClose(_xrdFd);
    } else if(_realFd != 0) {
        ::close(_realFd);
        _realFd = 0;
    }
}

bool PacketIter::incrementExtend() {
    LOGGER_DBG << "packetiter Realloc to " 
               << _current.second + _fragSize << std::endl;
    void* ptr = ::realloc(_current.first, _current.second + _fragSize);
    if(!ptr) {
        errno = ENOMEM;
        throw "Failed to realloc for PacketIter.";
    }
    _buffer = ptr;
    _current.first = static_cast<char*>(ptr);
    Value secondHalf(_current.first + _current.second, _fragSize);
    _fill(secondHalf);
    _current.second += secondHalf.second;
    if(secondHalf.second == 0) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////
// PacketIter private methods
////////////////////////////////////////////////////////////////////////
void PacketIter::_setup(bool debug) {
    _errno = 0; // Important to initialize for proper error handling.
    const int minFragment = 65536;
    _memo = false;
    if(!debug && (_fragSize < minFragment)) _fragSize = minFragment;

    assert(sizeof(char) == 1);
    assert(_current.first == 0);
    assert(_fragSize > 0);
    // malloc() is used here rather than the "new" operator because a low-level
    // bucket of bytes is desired.
    _buffer = malloc(_fragSize);
    if(_buffer == NULL) {
        errno = ENOMEM;
        throw "Failed to malloc for PacketIter.";
    }
    if(!_fileName.empty()) {
        _realFd = open(_fileName.c_str(), O_RDONLY);
        if(_realFd < 0) {
            _current.second = 0;
            _errno = errno;
            return;
        }
    }
    _current.second = _fragSize;
    _current.first = static_cast<char*>(_buffer);
    _fill(_current);
}

void PacketIter::_increment() {
    _pos += _current.second;
    _fill(_current);
}

void PacketIter::_fill(Value& v) {
    int readRes = 0;
    if(_stop) {
        v.first = 0;
        v.second = 0;
        return;
    }
    if(_xrdFd != 0) {
        readRes = xrdRead(_xrdFd, v.first, static_cast<unsigned long long>(v.second));
        if (readRes < 0) {
            throw "Remote I/O error during XRD read.";
        }
    } else if(!_fileName.empty()) {
        readRes = ::read(_realFd, v.first, v.second);
    } else {
        readRes = 0;
    }

    if(readRes < 0) {
        //Report error somehow
        _errno = errno;
    }
    if(readRes < static_cast<int>(v.second)) {
        _stop = true;
    }
    v.second = readRes;
}

}}} // namespace lsst::qserv::xrdc
