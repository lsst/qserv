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
#include "qcache/Page.h"

// Qserv headers
#include "qcache/Exceptions.h"

// System headers
#include <cstring>

namespace lsst {
namespace qserv {
namespace qcache {

std::shared_ptr<Page> Page::create(std::size_t capacityBytes) {
    return std::shared_ptr<Page>(new Page(capacityBytes));
}


Page::Page(std::size_t capacityBytes)
    :   _capacityBytes(capacityBytes),
        _data(new char[capacityBytes])  {
}


void Page::add(unsigned int numFields, char const** row, long const* lengths) {
    std::string const context = "Page::" + std::string(__func__) + " ";
    if (!numFields || !row || !lengths) {
        throw std::invalid_argument(context + "at least one of the input parameters has a value of 0.");
    }
    // Compute the amount of space needed to store the row (lengths + fields).
    auto const lengthsEnd = lengths + numFields;
    std::size_t const lengthSizeBytes = sizeof(long const);
    std::size_t numBytesRequired = numFields * lengthSizeBytes;
    for (auto ptr = lengths; ptr != lengthsEnd; ++ptr) {
        numBytesRequired += *ptr;
    }
    if (numBytesRequired > _capacityBytes - _sizeBytes) {
        throw PageOverflow("no more space in the page to store a row");
    }
    // Copy lengths of fields into the buffer.
    char* data = _data.get() + _sizeBytes;  // the moving pointer within the buffer
    for (auto ptr = lengths; ptr != lengthsEnd; ++ptr) {
        std::strncpy(data, (char const*)(&*ptr), lengthSizeBytes);
        data += lengthSizeBytes;
    }
    // Copy the fields
    for (unsigned int i = 0; i < numFields; ++i) {
        std::size_t const length = lengths[i];
        std::strncpy(data, (char const*)(row[i]), length);
        data += length;
    }
    _sizeBytes += data - _data.get();
    _sizeRows++;
}


}}} // namespace lsst::qserv::qcache
