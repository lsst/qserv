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
#ifndef LSST_QSERV_QCACHE_MYSQLCONNECTIONPOOL_H
#define LSST_QSERV_QCACHE_MYSQLCONNECTIONPOOL_H

// System headers
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>

// Forward declarations
namespace lsst {
namespace qserv {
namespace qcache {
    class MySQLConnection;
}}} // namespace lsst::qserv::qcache

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class MySQLConnectionPool is ...
  */
class MySQLConnectionPool: public std::enable_shared_from_this<MySQLConnectionPool> {
public:
    MySQLConnectionPool() = delete;
    MySQLConnectionPool(MySQLConnectionPool const&) = delete;
    MySQLConnectionPool& operator=(MySQLConnectionPool const&) = delete;

    virtual ~MySQLConnectionPool() = default;

    /**
     * Create the pool with the specified limits.
     */
    static std::shared_ptr<MySQLConnectionPool> create(std::size_t maxConnections=1);

    /**
     * Allocate a connection. If no free connections are available the method would have to wait
     * before some other thread will release the one.
     */
    std::shared_ptr<MySQLConnection> allocate();

    /**
     * Return a connection back to the pool.
     * @note mysql_connection_reset() will be called on the connection by this method.
     */
    void release(std::shared_ptr<MySQLConnection> const& connection);

private:
    MySQLConnectionPool(std::size_t maxConnections);

    // Parameters
    std::size_t const _maxConnections;

    /// Primitives for implementing synchronized metods.
    mutable std::condition_variable _cv;
    mutable std::mutex _mtx;

    /// The total number of allocated connections (not to exceed the limit
    /// specified in the member variable _maxConnections).
    std::size_t _numConnections = 0;

    /// The collection of the available connections. Up to the _maxConnections
    /// connections can exist in the list. Connections get removed from the list
    /// once they're allocated.
    /// @note Using the deque to ensure all connections are being resused. This would
    ///   minimize chances MySQL would close connections due to inactivity.
    std::deque<std::shared_ptr<MySQLConnection>> _free;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_MYSQLCONNECTIONPOOL_H
