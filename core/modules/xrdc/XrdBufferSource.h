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

// XrdBufferSource.h:
// An iterator that provides iteration over arbitrarily-sized pieces
// of a stream. Access either a local file or an xrootd file
// descriptor. Facilitates transferring bytes directly from the xrootd
// realm to a fragment consumer (probably the table merger). Allowing
// both types input sources makes it easier to reduce buffering and
// disk usage, theoretically improving overall latency.
//
#ifndef LSST_QSERV_XRDC_XRDBUFFERSOURCE_H
#define LSST_QSERV_XRDC_XRDBUFFERSOURCE_H

// System headers
#include <string>
#include <utility>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Qserv headers
#include "util/PacketBuffer.h"

namespace lsst {
namespace qserv {
namespace xrdc {

/// XrdBufferSource is a backend for a PacketBuffer, supplying bytes from an
/// xrootd file descriptor.
class XrdBufferSource : public util::PacketBuffer::Source {
public:
    typedef util::PacketBuffer::Value Value;

    XrdBufferSource(int xrdFd, int fragmentSize);

    /// For debugging.
    XrdBufferSource(std::string const& fileName,
                    int fragmentSize, bool debug);

    virtual ~XrdBufferSource();

    virtual Value getFirstValue();
    virtual void increment(util::PacketBuffer& p);
    virtual bool incrementExtend(util::PacketBuffer& p);
    virtual int getErrno() const { return _errno; }

private:
    void _setup(bool debug);
    void _increment();
    void _fill(char*& buf, int& len);

    int _xrdFd;
    std::string _fileName;
    char* _buffer;
    int _fragSize;
    bool _started;
    bool _memo;
    Value _current;
    bool _stop;
    int _occupiedSize;
    int _errno;
    int _realFd;
};

}}} // namespace lsst::qserv::xrdc
#endif //  LSST_QSERV_XRDC_XRDBUFFERSOURCE_H
