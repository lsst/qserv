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
#ifndef LSST_QSERV_QCACHE_RESULTMANAGER_H
#define LSST_QSERV_QCACHE_RESULTMANAGER_H

// System headers
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Forward declarations
namespace lsst {
namespace qserv {
namespace qcache {
    class MySQLConnection;
    class MySQLConnectionPool;
    class PageData;
    class PageHolder;
}}} // namespace lsst::qserv::qcache

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class ResultManager is for managing resuts sets of queries.
  */
class ResultManager: public std::enable_shared_from_this<ResultManager> {
public:
    ResultManager() = delete;
    ResultManager(ResultManager const&) = delete;
    ResultManager& operator=(ResultManager const&) = delete;

    virtual ~ResultManager() = default;

    /**
     * Create the ResultManager for operations with queries in a scope of the given
     * query/task.
     * @param queryId An identifier of the Qserv query.
     * @param taskId An identifier of a task (specific to chunk).
     * @param connPool The pool to acquire a MySQL connection.
     * @param io_service The service for asynchronous timer operations.
     * @param expirationTimeSec The optional result expiration timeout. Zero means
     *   no expiration timeout was requests. The expiration time (if requested) will
     *   start as soon as the query (queries) has finished being executed by MySQL.
     */
    static std::shared_ptr<ResultManager> create(std::string const& queryId,
                                                 std::string const& taskId,
                                                 std::shared_ptr<MySQLConnectionPool> const& connPool,
                                                 std::shared_ptr<Pool> const& pagePool,
                                                 boost::asio::io_service& io_service,
                                                 std::size_t expirationTimeoutSec=0);

    /// Execute a single query.
    /// @note exceptions are thrown for failures
    void execute(std::string const& query);

    /// @return 'true' if not in any failure state.
    bool isGood() const;

    /// @return 'true' if the object is in the good state (see method isGood)
    ///   and the result is (at least partially) available.
    bool isReady() const;

    /// @return 'true' if the object is in the good state (see method isGood) and
    ///   the complete result has been read from MySQL into the in-memory buffer
    ///   or written onto the disk cache.
    bool isComplete() const;

    /// Get the total number of bytes in the result size.
    /// @note the number returned by the method may be less then the actual size
    ///   while the result set is still being fetched from MySQL.
    /// @note exceptions are thrown if called in a wrong state of the object,
    ///   or for any errors occurred during the operation.
    std::size_t sizeBytes() const;

    /// Get the total number of rows in the result size.
    /// @note the number returned by the method may be less then the actual size
    ///   while the result set is still being fetched from MySQL.
    /// @note exceptions are thrown if called in a wrong state of the object,
    ///   or for any errors occurred during the operation..
    std::size_t sizeRows() const;

    /// Get the total number of pages in the result size.
    /// @note the number returned by the method may be less then the actual size
    ///   while the result set is still being fetched from MySQL.
    /// @note exceptions are thrown if called in a wrong state of the object,
    ///   or for any errors occurred during the operation..
    std::size_t sizePages() const;

    /**
     * Get the specified data page of the result set.
     * @note It's possible to read "future" pages while the result set is still
     *   being transferred from MySQL. In that case the method will get blocked
     *   waiting for the desired page to be ready. Exceptions will be thrown
     *   in case of any abnormalities encountered while waiting for the page,
     *   such as any failures to read the result from MySQL, result expiration,
     *   or shorter than expected by a caller of the method read.
     *   It's possible to call this method if the object is in the 'ready' state
     *   as reported by the corresponding method defined above.
     * @param pageIdx Zero-based index of a page.
     * @see method resultSizePages()
     */
    std::shared_ptr<PageData> page(std::size_t pageIdx);

    /// Release all resources, clear in-memory and persistent cache, release
    /// the connection back to the pool.
    void clear();

    friend class PageData;

private:
    ResultManager(std::string const& queryId,
                  std::string const& taskId,
                  std::shared_ptr<MySQLConnectionPool> const& connPool,
                  std::shared_ptr<Pool> const& pagePool,
                  boost::asio::io_service& io_service,
                  std::size_t expirationTimeoutSec);

    /**
     * The method is called by the d-tor of class PageData to notify the manager that
     * the page is no longer needed by a client.
     * @param padeIdx The page to be released.
     */
    void release(std::size_t pageIdx);

    /**
     * Verify if the page index is valid.
     * @param lock The lock on the objects state to be acquired before calling the method.
     * @param func The name of the calling function for reporting a context from which
     *   the metgod was called.
     * @param pageIdx The page index to be verified.
     * @throws std::out_of_range If the index is out of range.
     */
    void _assertPageIsValid(std::lock_guard const& lock,
                            std::string const& func,
                            std::size_t pageIdx) const;

    // Parameters
    std::string const _queryId;
    std::string const _taskId;
    std::shared_ptr<MySQLConnectionPool> const _connPool;
    std::shared_ptr<Pool> const _pagePool;
    boost::asio::io_service& io_service;
    std::size_t const expirationTimeoutSec;

    /// Primitives for implementing synchronized metods.
    mutable std::condition_variable _cv;
    mutable std::mutex _mtx;

    /// The internal state of the manager.
    enum State {
        INITIAL = 0,
        EXECUTING,
        EXECUTED,
        EXECUTE_FAILED,         // (final state) in case if any problem reported by MySQL
        FETCHING_RESULT,        // when reading result from MySQL into the in-memory buffer
        FETCH_RESULT_OVERFLOW,  // (final state) if the result is too big
        FETCH_RESULT_FAILED,    // (final state) for any other error when reading data from MySQL
        RESULT_IN_MEMORY,       // result is ready in the in-memory buffer
        WRITING_RESULT,         // result is being saved onto the disk cache
        RESULT_ON_DISK,         // result is written into the in memory disk
        EXPIRED,                // (final state) due to the timer expiration
        CLEARED                 // (final state) due to an explicit request
    };
    State _state = State::INITIAL;

    /// The connection is allocated from the pool when the method execute() gets called.
    /// It's released back to the poll after reading all data (or a failure to do so).
    std::shared_ptr<MySQLConnection> _conn;

    /// Storage for the pages.
    std::vector<std::shared_ptr<PageHolder>> _pages;

    /// The total number of rows in the result size. It's unknown before
    /// the result has been read.
    std::size_t _resultSetBytes = 0;

    /// The total number of rows in the result size.
     std::size_t _resultSetRows = 0;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_RESULTMANAGER_H
