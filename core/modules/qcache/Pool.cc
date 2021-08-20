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
#include "qcache/Pool.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "qcache/Page.h"

namespace lsst {
namespace qserv {
namespace qcache {

std::shared_ptr<Pool> Pool::create(std::size_t pageCapacityBytes,
                                   std::size_t maxNumPages) {
    return std::shared_ptr<Pool>(new Pool(pageCapacityBytes, maxNumPages));
}


Pool::Pool(std::size_t pageCapacityBytes, std::size_t maxNumPages)
    :   _pageCapacityBytes(pageCapacityBytes),
        _maxNumPages(maxNumPages) {
}


std::shared_ptr<Page> Pool::allocate() {
    return nullptr;
}


void Pool::release(std::shared_ptr<Page> const& page) {
    ;
}

}}} // namespace lsst::qserv::qcache
