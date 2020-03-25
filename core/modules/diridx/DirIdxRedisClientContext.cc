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
#include "DirIdxRedisClientContext.h"

// System headers

// Third-party headers
#include <boost/asio.hpp>

// Qserv headers

namespace lsst {
namespace qserv {
namespace diridx {


DirIdxRedisClientContext::DirIdxRedisClientContext(std::string const& name):
        _ioWorkGuard(boost::asio::make_work_guard(_io_service)),
        _dirIdxRedisClient(name, _io_service) {
    _workThread = std::thread([this] () { _io_service.run(); });
}


DirIdxRedisClientContext::~DirIdxRedisClientContext() {
    _ioWorkGuard.reset();
    _workThread.join();
}


}}}
