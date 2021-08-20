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
#include "qcache/MySQLConnectionPool.h"

// Qserv headers
#include "qcache/MySQLConnection.h"

// System headers
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace qcache {

std::shared_ptr<MySQLConnection> MySQLConnectionPool::create(std::size_t maxConnections) {
    return std::shared_ptr<MySQLConnectionPool>(new MySQLConnectionPool(maxConnections));
}


MySQLConnectionPool::MySQLConnectionPool(std::size_t maxConnections)
    :   _maxConnections(maxConnections) {
}


std::shared_ptr<MySQLConnection> MySQLConnectionPool::allocate() {
    std::unique_lock<std::mutex> lock(_mtx);
    if (_free.empty()) {
        if (_numConnections < _maxConnections) {
            ++_numConnections;
            return MySQLConnection::create();
        }
        _cv.wait(lock, [&]() { return !_free.empty(); });
    }
    auto conn = _free.front();
    _free.pop_front();
}


void MySQLConnectionPool::release(std::shared_ptr<MySQLConnection> const& conn) {
    std::unique_lock<std::mutex> lock(_mtx);
    if (conn == nullptr) {
        throw std::invalid_argument(
                "MySQLConnectionPool::" + std::string(__func__) + " null pointer passed into the method.");
    }
    _free.push_back(conn);
    if (_free.size() == 1) {
        lock.unlock();
        _cv.notify_one();
    }
}

}}} // namespace lsst::qserv::qcache
