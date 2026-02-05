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
 * by the MySQL-backed implementation of the database service.
 * @note See individual class documentation for more information.
 */

// System headers
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

// This header declarations
namespace lsst::qserv::replica::database::mysql {

/**
 * Class Error represents a family of exceptions which are specific
 * to the implementation of this API.
 */
class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Thrown after failing to connect to a server
 */
class ConnectError : public Error {
public:
    using Error::Error;
};

/**
 * Thrown if the connection attempt to a server failed to be established
 * within the specified timeout.
 */
class ConnectTimeoutError : public Error {
public:
    /**
     * @param what A reason for the exception.
     * @param timeoutSec A value of the timeout which expired.
     */
    ConnectTimeoutError(std::string const& what, unsigned int timeoutSec)
            : Error(what), _timeoutSec(timeoutSec) {}

    /// @return A value of the timeout that expired.
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
     * @param what A reason for the exception.
     * @param maxReconnects The number of reconnects which was set as a limit.
     */
    MaxReconnectsExceeded(std::string const& what, unsigned int maxReconnects)
            : Error(what), _maxReconnects(maxReconnects) {}

    /// @return The number of reconnects which was set as a limit.
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
    using Error::Error;
};

/// Thrown on attempts to create an index with the name of a key that already
/// exists in a table.
class ER_DUP_KEYNAME_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to insert rows with duplicate keys.
class ER_DUP_ENTRY_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to drop a field or a index that doesn't exist.
class ER_CANT_DROP_FIELD_OR_KEY_ : public Error {
public:
    using Error::Error;
};

/// Thrown on queries involving non-existing databases.
class ER_BAD_DB_ERROR_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to create existing databases.
class ER_DB_CREATE_EXISTS_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to drop non-existing databases.
class ER_DB_DROP_EXISTS_ : public Error {
public:
    using Error::Error;
};

/// Thrown on unauthorized attempts to access databases w/o any password.
class ER_DBACCESS_DENIED_ERROR_ : public Error {
public:
    using Error::Error;
};

/// Thrown on unauthorized attempts to access databases w/ a password.
class ER_ACCESS_DENIED_ERROR_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to create existing tables.
class ER_TABLE_EXISTS_ERROR_ : public Error {
public:
    using Error::Error;
};

/// Thrown on attempts to drop non-existing tables.
class ER_BAD_TABLE_ERROR_ : public Error {
public:
    using Error::Error;
};

/// Thrown on queries involving non-existing tables.
class ER_NO_SUCH_TABLE_ : public Error {
public:
    using Error::Error;
};

/**
 * Thrown on a specific query that attempts to remove all partitions from
 * a non-partitioned table:
 * @code
 *   ALTER TABLE <database>.<table> REMOVE PARTITIONING;
 * @code
 * Some application may choose to explicitly identify and process this type
 * of failures.
 */
class ER_PARTITION_MGMT_ON_NONPARTITIONED_ : public Error {
public:
    using Error::Error;
};

/**
 * Thrown on queries attempting to select data from a non-existing partitions
 * of a partitioned table:
 * @code
 *   SELECT * FROM <database>.<table> PARTITION (<partition>);
 * @code
 * Some application may choose to explicitly identify and process this type
 * of failures.
 */
class ER_UNKNOWN_PARTITION_ : public Error {
public:
    using Error::Error;
};

/**
 * Thrown on queries attempting to drop a non-existing partition from
 * the table's definition:
 * @code
 *   ALTER TABLE <database>.<table> DROP PARTITION <partition>;
 * @code
 */
class ER_DROP_PARTITION_NON_EXISTENT_ : public Error {
public:
    using Error::Error;
};

/**
 * Thrown in a scenario when deadlock found when trying to get lock. A solution
 * is to try restarting an ongoing transaction.
 */
class ER_LOCK_DEADLOCK_ : public Error {
public:
    using Error::Error;
};

/**
 * Thrown on foreign key constraint violations. MySQL reports this error when you try to add
 * a row but there is no parent row, and a foreign key constraint fails. Add the parent row first.
 * This exception may also indicate the normal scenario when a parent row was deleted on purpose.
 * In this cases, the application shoiuld catch this exception and handle it appropriately.
 */
class ER_NO_REFERENCED_ROW_2_ : public Error {
public:
    using Error::Error;
};

/**
 * Instances of this exception class are thrown on failed attempts
 * to interpret the contents of the result set.
 */
class InvalidTypeError : public Error {
public:
    using Error::Error;
};

/**
 * Instances of this exception class are thrown on empty result sets
 * by some methods when a query is supposed to return at least one row.
 */
class EmptyResultSetError : public Error {
public:
    using Error::Error;
};

}  // namespace lsst::qserv::replica::database::mysql

#endif  // LSST_QSERV_REPLICA_DATABASEMYSQLEXCEPTIONS_H
