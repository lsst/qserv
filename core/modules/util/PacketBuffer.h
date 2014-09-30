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
#ifndef LSST_QSERV_UTIL_PACKETBUFFER_H
#define LSST_QSERV_UTIL_PACKETBUFFER_H

// System headers
#include <string>
#include <utility>

// Third-party headers
#include "boost/shared_ptr.hpp"

namespace lsst {
namespace qserv {
namespace util {

/// An iterator that provides iteration over arbitrarily-sized pieces
/// of a stream. Access either a local file or an xrootd file
/// descriptor. Facilitates transferring bytes directly from the xrootd
/// realm to a fragment consumer (probably the table merger). Allowing
/// both types input sources makes it easier to reduce buffering and
/// disk usage, theoretically improving overall latency.
class PacketBuffer {
public:
    typedef boost::shared_ptr<PacketBuffer> Ptr;
    typedef std::pair<char*, unsigned> Value;
    typedef unsigned long long Pos;

    /// The interface for a backend to be plugged into a PacketBuffer
    class Source {
    public:
        virtual Value getFirstValue() = 0;
        virtual void increment(PacketBuffer& p) = 0;
        virtual bool incrementExtend(PacketBuffer& p) { return false; }
        virtual int getErrno() const { return 0; }
    protected:
        void setPos(PacketBuffer& p, Pos pos) { p._pos = pos; }
        void setCurrent(PacketBuffer&p, char* buf, unsigned len) {
            p._current.first = buf;
            p._current.second = len;
        }
    };
    friend class Source;

    /// A simple Source implementation on top of a fixed buffer.
    class FixedSource : public Source {
    public:
        FixedSource(char const* buf, unsigned size)
            : _orig(const_cast<char*>(buf), size) {}

        virtual Value getFirstValue() { return _orig; }
        virtual void increment(PacketBuffer& p) {
            // can increment only once: to end.
            setPos(p, _orig.second);
            setCurrent(p, 0, 0);
        }
        Value _orig;
    };
    /// Constructor.
    /// @param buf read-only buffer. unowned, does not take ownership
    /// @param size size of buffer
    PacketBuffer(char const* buf, int size)
        : _pos(0) {
        _source.reset(new FixedSource(buf, size));
        _current = _source->getFirstValue();
    }

    /// Construct a PacketBuffer with a specified backend.
    /// Takes ownership of Source.
    PacketBuffer(Source* s)
        : _source(s), _pos(0) {
        _current = _source->getFirstValue();
    }

    virtual ~PacketBuffer() {}

    // Dereference
    const Value& operator*() const {
        return _current;
    }
    const Value* operator->() const {
        return &_current;
    }

    // Increment
    virtual PacketBuffer& operator++() {
        _source->increment(*this);
        return *this;
    }

    // Increment, but combine next packet into current buffer.
    // Result: iterator points at same place in the stream, but
    // current chunk is bigger.
    // @return false if could not extend.
    bool incrementExtend() { return _source->incrementExtend(*this); }

    // Const accessors:
    bool isDone() const { return _current.second == 0; }
    Pos getPos() const { return _pos; }
    int getErrno() const { return _source->getErrno();}
    ssize_t getTotalSize() const { return _pos + _current.second; }

private:
    /// Unimplemented. Shouldn't really be comparing.
    bool operator==(PacketBuffer const& rhs) const;
    /// Unimplemented. Copying this is not really safe without a lot
    /// more implementation.
    PacketBuffer operator++(int);

    std::auto_ptr<Source> _source;
    Pos _pos;
    Value _current;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_PACKETBUFFER_H
