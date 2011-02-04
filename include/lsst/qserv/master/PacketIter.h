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
#include <string>
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

    /// Constructor. 
    explicit PacketIter(int xrdFd, int fragmentSize=2*1024*1024);
    explicit PacketIter(std::string const& fileName, 
                        int fragmentSize=2*1024*1024, bool debug=false);
    ~PacketIter();

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

    // Increment, but combine next packet into current buffer.
    // Result: iterator points at same place in the stream, but 
    // current chunk is bigger.
    // @return false if could not extend.
    bool incrementExtend();

    // Const accessors:
    bool isDone() const { return _current.second == 0; }
    Pos getPos() const { return _pos; }
    int getErrno() const { return _errno;}
    ssize_t getTotalSize() const { return _pos + _current.second; }
private:
    void _setup(bool debug);
    void _increment();
    void _fill(Value& v);

    // Shouldn't really be comparing.
    bool operator==(PacketIter const& rhs) const;
    // Copying this is not really safe without a lot more implementation.
    PacketIter operator++(int);

    int _xrdFd;
    std::string _fileName;
    int _fragSize;
    bool _started;
    bool _memo;
    Value _current;
    bool _stop;
    void* _buffer;
    int _errno;
    Pos _pos;
    int _realFd;
};

}}} // lsst::qserv::master
 
#endif // LSST_QSERV_MASTER_PACKETITER_H
