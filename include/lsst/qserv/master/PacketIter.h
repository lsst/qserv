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

// PacketIter.h: 
// An iterator that provides iteration over arbitrarily-sized pieces of a 
// stream.
//
#ifndef LSST_QSERV_MASTER_PACKETITER_H
#define LSST_QSERV_MASTER_PACKETITER_H
#include <boost/shared_ptr.hpp>
#include <utility>

namespace lsst {
namespace qserv {
namespace master {

class PacketIter {
public:
    typedef boost::shared_ptr<PacketIter> Ptr;
    typedef std::pair<char*, unsigned> Value;
    typedef unsigned long long Pos;
    
    explicit PacketIter();

    /// Constructor.  Buffer must be valid over this object's lifetime.
    explicit PacketIter(int xrdFd, int fragmentSize=2*1024*1024);
    
    ~PacketIter() {
        if(_buffer != NULL) free(_buffer);
    }

    // Dereference
    const Value& operator*() const { 
        return _current; 
    }
    const Value* operator->() const { 
        return &_current; 
    }

    // Increment
    PacketIter& operator++() { 
        _increment();
        return *this;
    }

    // Const accessors:
    bool isDone() const { return _current.second == 0; }
    Pos getPos() const { return _pos; }
    int getErrno() const { return _errno;}
private:
    void _setup();
    void _increment();
    void _fill(Value& v);

    // Shouldn't really be comparing.
    bool operator==(PacketIter const& rhs) const;
    // Copying this is not really safe without a lot more implementation.
    PacketIter operator++(int);

    int _xrdFd;
    int _fragmentSize;
    bool _started;
    Value _current;
    bool _stop;
    void* _buffer;
    int _errno;
    Pos _pos;
};

}}} // lsst::qserv::master
 
#endif // LSST_QSERV_MASTER_PACKETITER_H
