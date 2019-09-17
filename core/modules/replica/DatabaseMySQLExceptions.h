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
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQLEXCEPTIONS_H
#define LSST_QSERV_REPLICA_DATABASEMYSQLEXCEPTIONS_H

/**
 * DatabaseMySQLExceptions.h declares a collection of exceptions thrown
 * by the MySQL-backed implementation of the database service:
 *
 * class Error
 * class ConnectError
 * class ConnectTimeout
 * class Reconnected
 * class DuplicateKeyError
 * class InvalidTypeError
 * class EmptyResultSetError
 *
 * (see individual class documentation for more information)
 */

// System headers
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/**
 * Class Error represents a family of exceptions which are specific
 * to the implementation of this API.
 */
class Error : public std::runtime_error {
public:
    /// @param what  reason for the exception
    explicit Error(std::string const& what) : std::runtime_error(what) {}
};


/**
 * Thrown after failing to connect to a server
 */
class ConnectError : public Error {
public:
    /// @param what  reason for the exception
    explicit ConnectError(std::string const& what) : Error(what) {}
};

/**
 * Thrown if the connection attempt to a server failed to be established
 * within the specified timeout.
 */
class ConnectTimeout : public Error {
public:
    /**
     * @param what        reason for the exception
     * @param timeoutSec  a value of the timeout which expired
     */
    ConnectTimeout(std::string const& what,
                   unsigned int timeoutSec)
        :   Error(what),
            _timeoutSec(timeoutSec) {
    }

    /// @return a value of the timeout
    unsigned int timeoutSec() const { return _timeoutSec; }

private:
    unsigned int _timeoutSec = 0;
};


/**
 * Thrown after exceeding an allowed number of failed connection attempts
 * to a server.
 */
class MaxReconnectsExceeded : public Error {
public:
    /**
     * @param what           reason for the exception
     * @param maxReconnects  the number of reconnects which was set as a limit
     */
    MaxReconnectsExceeded(std::string const& what,
                          unsigned int maxReconnects)
        :   Error(what),
            _maxReconnects(maxReconnects) {
    }

    /// @return the number of reconnects which was set as a limit
    unsigned int maxReconnects() const { return _maxReconnects; }

private:
    unsigned int _maxReconnects = 0;
};


/**
 * Thrown after a successful reconnection to a server. Normally, after catching
 * this exception, an application should repeat the last attempted transaction.
 * It's guaranteed that all traces of the failed transaction were properly
 * cleaned up.
 */
class Reconnected : public Error {
public:
    /// @param what  reason for the exception
    explicit Reconnected(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on attempts to insert
 * rows with duplicate keys.
 */
class DuplicateKeyError : public Error {
public:
    /// @param what  reason for the exception
    explicit DuplicateKeyError(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on queries involving non-existing
 * tables.
 */
class NoSuchTable : public Error {
public:
    /// @param what  reason for the exception
    explicit NoSuchTable(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on a specific query which
 * attempts to remove all partitions from a non-partitioned table:
 * 
 *   @code
 *   ALTER TABLE <database>.<table> REMOVE PARTITIONING;
 *   @code
 * 
 * Some application may choose to explicitly identify and process this type
 * of failures.
 */
class NotPartitionedTable : public Error {
public:
    /// @param what  reason for the exception
    explicit NotPartitionedTable(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on queries attempting to select
 * data from a non-existing partitions of a partitioned table:
 * 
 *   @code
 *   SELECT * FROM <database>.<table> PARTITION (<partition>);
 *   @code
 * 
 * Some application may choose to explicitly identify and process this type
 * of failures.
 */
class NoSuchPartition : public Error {
public:
    /// @param what  reason for the exception
    explicit NoSuchPartition(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on failed attempts
 * to interpret the contents of the result set.
 */
class InvalidTypeError : public Error {
public:
    /// @param what  reason for the exception
    explicit InvalidTypeError(std::string const& what) : Error(what) {}
};


/**
 * Instances of this exception class are thrown on empty result sets
 * by some methods when a query is supposed to return at least one row.
 */
class EmptyResultSetError : public Error {
public:
    /// @param what  reason for the exception
    explicit EmptyResultSetError(std::string const& what) : Error(what) {}
};


}}}}} // namespace lsst::qserv::replica::database::mysql

#endif // LSST_QSERV_REPLICA_DATABASEMYSQLEXCEPTIONS_H
