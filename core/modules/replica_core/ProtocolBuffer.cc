/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

#include "replica_core/ProtocolBuffer.h"

// System headers

#include <algorithm>

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica_core {

const size_t
ProtocolBuffer::DESIRED_LIMIT = 2000000;

const size_t
ProtocolBuffer::HARD_LIMIT = 64000000;


ProtocolBuffer::ProtocolBuffer (size_t capacity)
    :   _data    (new char[capacity]),
        _capacity(capacity),
        _size    (0)
{
    if (_capacity > HARD_LIMIT)
        throw std::overflow_error("ProtocolBuffer::ProtocolBuffer() requested capacity " + std::to_string(capacity) +
                                  " exceeds the hard limit of Google protobuf: " + std::to_string(HARD_LIMIT));
}

ProtocolBuffer::~ProtocolBuffer () {
    delete [] _data;
    _data = 0;
    _capacity = 0;
    _size = 0;
}

void
ProtocolBuffer::resize (size_t newSizeBytes) {

    // Make sure there is enough space in the buffer to accomodate
    // the request.

    extend(newSizeBytes);

    _size = newSizeBytes;
}

void
ProtocolBuffer::extend (size_t newCapacityBytes) {

    if (newCapacityBytes <= _capacity) return;

    // Allocate a larger buffer

    if (newCapacityBytes > HARD_LIMIT)
        throw std::overflow_error("ProtocolBuffer::extend() requested capacity " + std::to_string(newCapacityBytes) +
                                  " exceeds the hard limit of Google protobuf " + std::to_string(HARD_LIMIT));

    char* ptr = new char[newCapacityBytes];
    if (!ptr)
        throw std::overflow_error("ProtocolBuffer::extend() failed to allocate a buffer of requested size " +
                                  std::to_string(newCapacityBytes));

    // Carry over the meaningful content of the older buffer into the new one
    // before disposing the old buffer.

    std::copy(_data, _data + _size, ptr);

    delete [] _data;
    _data = ptr;

    _capacity = newCapacityBytes;
}

uint32_t
ProtocolBuffer::parseLength () const {

    if (_size != sizeof(uint32_t))
        std::overflow_error("not enough data to be interpreted as the frame header");

    return ntohl(*(reinterpret_cast<const uint32_t*>(_data)));
}

}}} // namespace lsst::qserv::replica_core
