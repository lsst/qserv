/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

#include "proto/FrameBuffer.h"

// System headers

#include <algorithm>

// Qserv headers

namespace lsst {
namespace qserv {
namespace proto {


///////////////////////////
// Class FrameBufferView //
///////////////////////////

FrameBufferView::FrameBufferView (char const* data,
                                  size_t      size)
    :   _data (data),
        _next (data),
        _size (size)
{ }

FrameBufferView::~FrameBufferView () {
}


uint32_t
FrameBufferView::parseLength () {

    uint32_t const headerLength = sizeof(uint32_t);
    if (_size - (_next - _data) < headerLength)
        FrameBufferError (
            "FrameBufferView::parseLength()  ** not enough data to be interpreted as the frame header **");

    uint32_t const messageLength = ntohl(*(reinterpret_cast<const uint32_t*>(_next)));

    // Move the pointer to the next message (if any)
    _next += headerLength;

    return messageLength;
}


///////////////////////
// Class FrameBuffer //
///////////////////////

const size_t FrameBuffer::DEAFULT_SIZE  = 1024;
const size_t FrameBuffer::DESIRED_LIMIT = 2000000;
const size_t FrameBuffer::HARD_LIMIT    = 64000000;

FrameBuffer::FrameBuffer (size_t capacity)
    :   _data    (new char[capacity]),
        _capacity(capacity),
        _size    (0)
{
    if (_capacity > HARD_LIMIT)
        throw FrameBufferError (
                "FrameBuffer::FrameBuffer()  ** requested capacity " + std::to_string(capacity) +
                " exceeds the hard limit of Google protobuf: " + std::to_string(HARD_LIMIT) +
                " **");
}

FrameBuffer::~FrameBuffer () {
    delete [] _data;
    _data = 0;
    _capacity = 0;
    _size = 0;
}

void
FrameBuffer::resize (size_t newSizeBytes) {

    // Make sure there is enough space in the buffer to accomodate
    // the request.

    extend(newSizeBytes);

    _size = newSizeBytes;
}

void
FrameBuffer::extend (size_t newCapacityBytes) {

    if (newCapacityBytes <= _capacity) return;

    // Allocate a larger buffer

    if (newCapacityBytes > HARD_LIMIT)
        throw FrameBufferError (
                "FrameBuffer::extend()  ** requested capacity " + std::to_string(newCapacityBytes) +
                " exceeds the hard limit of Google protobuf " + std::to_string(HARD_LIMIT) +
                " **");

    char* ptr = new char[newCapacityBytes];
    if (!ptr)
        throw FrameBufferError (
                "FrameBuffer::extend()  ** failed to allocate a buffer of requested size " +
                std::to_string(newCapacityBytes) +
                " **");

    // Carry over the meaningful content of the older buffer into the new one
    // before disposing the old buffer.
    std::copy(_data, _data + _size, ptr);

    delete [] _data;
    _data = ptr;

    _capacity = newCapacityBytes;
}

}}} // namespace lsst::qserv::proto
