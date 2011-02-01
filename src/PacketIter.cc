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

#include "lsst/qserv/master/PacketIter.h"
#include "lsst/qserv/master/xrdfile.h"

namespace qMaster = lsst::qserv::master;

qMaster::PacketIter::PacketIter() 
  : _xrdFd(-1), _current(0,0), _stop(false) 
{}

qMaster::PacketIter::PacketIter(int xrdFd, int fragmentSize) 
    : _xrdFd(xrdFd), 
      _fragSize(fragmentSize),
      _current(0,0), 
      _stop(false) {
    const int minFragment = 65536;
    if(_fragSize < minFragment) _fragSize = minFragment;
}
    
void qMaster::PacketIter::_setup() {
    assert sizeof(char) == 1;
    assert _current.first == 0;
    assert _fragSize > 0;
    _buffer = malloc(_fragSize); // allocate space for two buffers.
    if(_buffer == NULL) {
        std::cerr << "Can't malloc for PacketIter. Raising exception." 
                  << std::endl;
        assert _buffer != NULL;
    }
    _current.first = _buffer;
    _fill(_current);
}

void qMaster::PacketIter::_increment() {
    _pos += _current.second;
    _fill(_current);
}

void qMaster::PacketIter::_fill(Value& v) {
    if(_stop) { 
        v.first = 0; 
        v.second = 0;
        return;
    }
    int readRes = xrdRead(_xrdFd, v.first, 
                          static_cast<unsigned long long>(_fragSize));
    if(readRes < 0) {
        //Report error somehow
        _errno = errno;
    } 
    v.second = readRes;
    if(v.second < _fragSize) {
        _stop = true;
    }

}

int _xrdFd;


