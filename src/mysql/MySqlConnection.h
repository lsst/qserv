// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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
// MySqlConnection represents an abstracted interface to the mysql C-API.
// Each MySqlConnection object is not parallel, but multiple objects can be used
// to achieve parallel query streams.

#ifndef LSST_QSERV_MYSQL_MYSQLCONNECTION_H
#define LSST_QSERV_MYSQL_MYSQLCONNECTION_H

// System headers
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Third-party headers
#include <mysql/mysql.h>

// Forward declarations
namespace lsst::qserv::mysql {
class MySqlConfig;
}  // namespace lsst::qserv::mysql

namespace lsst::qserv::mysql {

/// MySqlConnection is a thin wrapper around the MySQL C-API that partially
/// shields clients from the raw API, while still providing raw access for
/// clients that need it.
class MySqlConnection {
public:
    /// The completion status of the query cancelation operation.
    enum CancelStatus {
        CANCEL_SUCCESS = 0,        ///< The operation was succesfull.
        CANCEL_CONNECT_ERROR = 1,  ///< Failed to establish a separate connection to MySQL.
        CANCEL_FAILED = 2,         ///< Failed to failure to kill the query.
        CANCEL_NOP = -1            ///< Connection is not open.
    };

    /**
     * Check if a MySQL connection could be established for the given configuration.
     * @return 'true' if MySQL connection succeeded.
     */
    static bool checkConnection(mysql::MySqlConfig const& config);

    /**
     * Construct the connector with specifid configuration.
     * @param sqlConfig Parameters of the connection.
     */
    explicit MySqlConnection(MySqlConfig const& config);

    MySqlConnection() = delete;
    MySqlConnection(MySqlConnection const&) = delete;
    MySqlConnection& operator=(MySqlConnection const&) = delete;

    /// Non-trivial destructor is needed to close the connection and release resources.
    ~MySqlConnection();

    MySqlConfig const& getConfig() const { return *_config; }

    /// Close the current connection (if any) and open the new one.
    /// @return 'true' if the operation was succesfull.
    bool connect();

    bool connected() const { return nullptr != _mysql; }

    /// @note The identifier is set after making a connection, and it's reset
    ///   to 0 upon disconnects.
    /// @return A thread identifier of the last succesfully established connection.
    unsigned long threadId() const { return _threadId.load(); }

    /// Close the current connection (if any).
    void closeMySqlConn();

    /**
     * Execute a query.
     * @param query The query to be executed.
     * @return 'true' if the operation was successfull.
     */
    bool queryUnbuffered(std::string const& query);

    /**
     * Cancel existing query (if any).
     * @note The method will only attempt to cancel the ongoing query (if any).
     *   The connection (if any) will be left intact, and it could be used for
     *   submitting other queries.
     * @return CancelStatus The completion status of the operation.
     */
    CancelStatus cancel();

    // The following methods require a valid connection.
    // Otherwise std::logic_error will be thrown.

    MYSQL* getMySql();
    unsigned int getErrno() const;
    const std::string getError() const;
    bool selectDb(std::string const& dbName);

    /**
     * The method requires a valid connection.
     * @return A pointer to the result descriptor. The method returns nullptr
     * if the last query failed, if no query submitted after establishing a connection,
     * or if the result set of the last query was explicitly cleared by calling freeResult().
     * @throws std::logic_error if the connection is not open.
     */
    MYSQL_RES* getResult();

    // The following methods require must be called within the query procesing
    // context (assuming the connection is open and the last query succeeded).
    // Otherwise std::logic_error will be thrown.

    void freeResult();
    int getResultFieldCount();
    std::vector<std::string> getColumnNames() const;

    /// @return a string suitable for logging.
    std::string dump();

private:
    /// Close the current connection (if open).
    /// @param lock An exclusive lock on the _mtx must  be acquired before calling the method.
    void _closeMySqlConnImpl(std::lock_guard<std::mutex> const& lock);

    /// Ensure a connection is establushed.
    /// @param func A context the method was called from (for error reporting).
    /// @throw std::logic_error If not in the desired state.
    void _throwIfNotConnected(std::string const& func) const;

    /// Ensure the object in the result processing state (a connection is established and
    /// the last submitted query succeeded).
    /// @param func A context the method was called from (for error reporting).
    /// @throw std::logic_error If not in the desired state.
    void _throwIfNotInProcessingResult(std::string const& func) const;

    std::shared_ptr<MySqlConfig> _config;  ///< Input parameters of the connections.

    mutable std::mutex _mtx;  ///< Guards state transitions.

    // The current state of the connection. Values of the data members
    // are modified after establishing a connection, query completion, or
    // uppon disconnects.

    MYSQL* _mysql;
    MYSQL_RES* _mysql_res;
    std::atomic<unsigned long> _threadId{0};
};

}  // namespace lsst::qserv::mysql

#endif  // LSST_QSERV_MYSQL_MYSQLCONNECTION_H
