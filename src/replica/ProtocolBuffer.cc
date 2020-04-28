/*
 * LSST Data Management System
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
#include "replica/ProtocolBuffer.h"

// System headers
#include <algorithm>

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

size_t const ProtocolBuffer::DESIRED_LIMIT =  2000000;
size_t const ProtocolBuffer::HARD_LIMIT    = 64000000;


ProtocolBuffer::ProtocolBuffer(size_t capacity)
    :   _data(new char[capacity]),
        _capacity(capacity),
        _size(0) {

    if (_capacity > HARD_LIMIT) {
        throw overflow_error(
                "ProtocolBuffer::" + string(__func__) +
                "  requested capacity " + to_string(capacity) +
                " exceeds the hard limit of Google protobuf: " + to_string(HARD_LIMIT));
    }
}


ProtocolBuffer::~ProtocolBuffer() {
    delete [] _data;
    _data = 0;
    _capacity = 0;
    _size = 0;
}


void ProtocolBuffer::resize(size_t newSizeBytes) {

    // Make sure there is enough space in the buffer to accommodate
    // the request.

    _extend(newSizeBytes);

    _size = newSizeBytes;
}


void ProtocolBuffer::_extend(size_t newCapacityBytes) {

    if (newCapacityBytes <= _capacity) return;

    // Allocate a larger buffer

    if (newCapacityBytes > HARD_LIMIT) {
        throw overflow_error(
                "ProtocolBuffer::" + string(__func__) + "  requested capacity " +
                to_string(newCapacityBytes) + " exceeds the hard limit of Google Protobuf " +
                to_string(HARD_LIMIT));
    }

    char* ptr = new char[newCapacityBytes];
    if (not ptr) {
        throw overflow_error(
                "ProtocolBuffer::" + string(__func__) +
                "  failed to allocate a buffer of requested size " + to_string(newCapacityBytes));
    }

    // Carry over the meaningful content of the older buffer into the new one
    // before disposing the old buffer.

    copy(_data, _data + _size, ptr);

    delete [] _data;
    _data = ptr;

    _capacity = newCapacityBytes;
}


uint32_t ProtocolBuffer::parseLength() const {

    if (_size != sizeof(uint32_t)) {
        overflow_error(
                "ProtocolBufferr::" + string(__func__) +
                "  not enough data to be interpreted as the frame header");
    }
    return ntohl(*(reinterpret_cast<uint32_t const*>(_data)));
}

}}} // namespace lsst::qserv::replica