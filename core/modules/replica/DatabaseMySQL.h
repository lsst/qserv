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
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQL_H
#define LSST_QSERV_REPLICA_DATABASEMYSQL_H

/**
 * This header represents a C++ wrapper for the MySQL C language library.
 * The primary class of this API is Connection.
 *
 * @see class Connection
 *
 * Other public classes of the API, such as class Row, specific exception
 * classes, as well as some others, are defined in separate headers
 * included from this one:
 *
 * DatabaseMySQLExceptions.h
 * DatabaseMySQLTypes.h
 * DatabaseMySQLRow.h
 */

// System headers
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

// Third party headers
#include <mysql/mysql.h>

// Qserv headers
#include "replica/Common.h"
#include "replica/DatabaseMySQLExceptions.h"
#include "replica/DatabaseMySQLTypes.h"
#include "replica/DatabaseMySQLRow.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ProtocolResponseSqlField;
}}} // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/**
 * Class Connection provides the main API to the database.
 */
class Connection : public std::enable_shared_from_this<Connection> {
public:
    typedef std::shared_ptr<Connection> Ptr;

    /// @return value of the corresponding MySQL variable set for a session
    static unsigned long max_allowed_packet();

    /**
     * Connect to the MySQL service with the specified parameters and if successfully
     * connected return a pointer to the Connection object. Otherwise an exception will
     * be thrown.
     *
     * A behavior of a connector created by the method depends on default values
     * of Configuration parameters returned by Configuration::databaseAllowReconnect()
     * and Configuration::databaseConnectTimeoutSec(). If the automatic reconnect is
     * allowed then multiple connection attempts to a database service can be made
     * before the connection timeout expires or until some problem which can't be
     * resolved with the allowed connection retries happens.
     *
     * @note MySQL auto-commits are disabled
     * @note MySQL automatic re-connects are not allowed because this Connector class
     *   implements its own protocol for reconnects (when allowed)
     * @note connections are always open with option MYSQL_PROTOCOL_TCP.
     * @note MySQL option MYSQL_OPT_LOCAL_INFILE is always enabled to allow
     *   queries like 'LOAD DATA LOCAL INFILE ...'. This protocol is used in the bulk
     *   data ingest scenarios in which input files can't be directly placed at a location
     *   accessible by the MySQL server. Be aware that this way of loading data requires
     *   the server to be configured with global variable 'local_infile=1'. Another related
     *   requirement is that MySQL client library was compiled with option '-DENABLED_LOCAL_INFILE=true'.
     *
     * Here is an example of using this method to establish a connection:
     * @code
     *    BlockPost delayBetweenReconnects(1000,2000);
     *    ConnectionParams params = ...;
     *    Connection::Ptr conn;
     *    do {
     *        try {
     *            conn = Connection::open(params);
     *            
     *        } catch (ConnectError const& ex) {
     *            cerr << "connection attempt failed: " << ex.what << endl;
     *            delayBetweenReconnects.wait();
     *            cerr << "reconnecting..." << endl;
     *            
     *        } catch (ConnectTimeoutError const& ex) {
     *            cerr << "connection attempt expired after: " << ex.timeoutSec() << " seconds "
     *                 << "due to: " << ex.what << endl;
     *            throw;
     *            
     *        } catch (Error const& ex) {
     *            cerr << "connection attempt failed: " << ex.what << endl;
     *            throw;
     *        }
     *   } while (nullptr != conn);
     * @code
     *
     * @param connectionParams parameters of a connection
     *
     * @return a valid object if the connection attempt succeeded (no nullptr
     *   to be returned under any circumstances)
     * 
     * @throw ConnectTimeoutError is thrown only if the automatic reconnects
     *   are allowed to indicate that connection attempts to a server
     *   failed to be established within the specified timeout
     *
     * @throw ConnectError the exception is thrown if automatic reconnets are not allowed
     *   to indicate that the only connection attempt to a server failed
     *
     * @throw Error - for any other database errors
     *
     * @see Configuration::databaseAllowReconnect()
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Connection::open2()
     */
    static Ptr open(ConnectionParams const& connectionParams);

    /**
     * The factory method allows to override default values of the corresponding
     * connection management options of the Configuration.
     *
     * @note If the timeout is set to 0 (the default value) and if reconnects are
     *   allowed then the method will assume a global value defined by
     *   the Configuration parameter: Configuration::databaseConnectTimeoutSec()
     * @note The same value of the timeout would be also assumed if the connection
     *   is lost when executing queries or pulling the result sets.
     * 
     * @param connectionParams
     * @param allowReconnects  if 'true' then multiple reconnection attempts will be allowed
     * @param connectTimeoutSec  maximum number of seconds to wait before a connection with
     *   a database server is established.
     *
     * @return a valid object if the connection attempt succeeded (no nullptr
     *  to be returned under any circumstances)
     *
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Connection::open()
     */
    static Ptr open2(ConnectionParams const& connectionParams,
                     bool allowReconnects=false,
                     unsigned int connectTimeoutSec=0);

    Connection() = delete;
    Connection(Connection const&) = delete;
    Connection& operator=(Connection const&) = delete;

    ~Connection();

    /// @return maximum amount of time to wait while making reconnection attempts
    unsigned int connectTimeoutSec() const { return _connectTimeoutSec; }

    std::string escape(std::string const& str) const;
    std::string charSetName() const;

    // -------------------------------------------------
    // Helper methods for simplifying query preparation
    // -------------------------------------------------

    template <typename T>
    T           sqlValue(T const&            val) const { return val; }
    std::string sqlValue(std::string const&  val) const { return "'" + escape(val) + "'"; }
    std::string sqlValue(char const*         val) const { return sqlValue(std::string(val)); }
    std::string sqlValue(DoNotProcess const& val) const { return val.name; }
    std::string sqlValue(Keyword      const& val) const { return val.name; }
    std::string sqlValue(Function     const& val) const { return val.name; }

    std::string sqlValue(std::vector<std::string> const& coll) const;

    /**
     * The function replaces the "conditional operator" of C++ in SQL statements
     * generators. Unlike the standard operator this function allows internal
     * type switching while producing a result of a specific type.
     *
     * @return an object which doesn't require any further processing
     */
    DoNotProcess nullIfEmpty(std::string const& val) {
        return val.empty() ? DoNotProcess(Keyword::SQL_NULL) : DoNotProcess(sqlValue(val));
    }

    // Generator: ([value [, value [, ... ]]])
    // Where values of the string types will be surrounded with single quotes

    /// The end of variadic recursion
    void sqlValues(std::string& sql) const { sql += ")"; }

    /// The next step in the variadic recursion when at least one value is
    /// still available
    template <typename T, typename ...Targs>
    void sqlValues(std::string& sql,
                   T val,
                   Targs... Fargs) const {
        bool const last = sizeof...(Fargs) - 1 < 0;
        std::ostringstream ss;
        ss << (sql.empty() ? "(" : (last ? "" : ",")) << sqlValue(val);
        sql += ss.str();

        // Recursively keep drilling down the list of arguments with one
        // argument less.
        sqlValues(sql, Fargs...);
    }

    /**
     * Turn values of variadic arguments into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     * @code
     *   sqlPackValues("st'r", std::string("c"), 123, 24.5);
     * @code
     *
     * This will produce the following output:
     * @code
     *   ('st\'r','c',123,24.5)
     * @code
     */
    template <typename...Targs>
    std::string sqlPackValues(Targs... Fargs) const {
        std::string sql;
        sqlValues(sql, Fargs...);
        return sql;
    }

    /**
     * Generate an SQL statement for inserting a single row into the specified
     * table based on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. Arguments of
     * types 'std::string' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * @param tableName the name of a table
     * @param Fargs the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlInsertQuery(std::string const& tableName,
                               Targs... Fargs) const {
        std::ostringstream qs;
        qs  << "INSERT INTO " << sqlId(tableName) << " "
            << "VALUES "      << sqlPackValues(Fargs...);
        return qs.str();
    }

    /// Return a string representing a built-in MySQL function for the last
    /// insert auto-incremented identifier: LAST_INSERT_ID()
    std::string sqlLastInsertId() const { return "LAST_INSERT_ID()"; }

    // ----------------------------------------------------------------------
    // Generator: [`column` = value [, `column` = value [, ... ]]]
    // Where values of the string types will be surrounded with single quotes

    /// Return a non-escaped and back-tick-quoted string which is meant
    /// to be an SQL identifier.
    std::string sqlId(std::string const& str) const { return "`" + str + "`"; }

    /// @return a composite identifier for a database and a table, or a table and a column
    std::string sqlId(std::string const& first, std::string const& second) const {
        return sqlId(first) + "." + sqlId(second);
    }

    /// @return a back-ticked identifier of a MySQL partition for the given "super-transaction"
    std::string sqlPartitionId(TransactionId transactionId) const {
        return sqlId("p" + std::to_string(transactionId));
    }

    /**
     * Generate and return an SQL expression for a binary operator applied
     * over a pair of a simple identifier and a value.
     *
     * @param col  the name of a column on the LHS of the expression
     * @param val  RHS value of the binary operation
     * @param op   binary operator to be applied to both above
     *
     * @return "<col> <binary operator> <value>" where column name will be
     *   surrounded by back ticks, and  values of string types will be escaped
     *   and surrounded by single quotes.
     */
    template <typename T>
    std::string sqlBinaryOperator(std::string const& col,
                                  T const& val,
                                  char const* op) const {
        std::ostringstream ss;
        ss << sqlId(col) << op << sqlValue(val);
        return ss.str();
    }
    
    /**
     * @return "<quoted-col> = <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "=");
    }

    /**
     * @return "<quoted-col> != <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlNotEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "!=");
    }

    /**
     * @return "<quoted-col> < <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLess(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "<");
    }

    /**
     * @return "<quoted-col> <= <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLessOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "<=");
    }

    /**
     * @return "<quoted-col> > <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreater(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, ">");
    }

    /**
     * @return "<quoted-col> => <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreaterOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, ">=");
    }

    /// The base (the final function) to be called
    void sqlPackPair(std::string&) const {}

    /// Recursive variadic function (overloaded for column names given as std::string)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string& sql,
                     std::pair<std::string,T> colVal,
                     Targs... Fargs) const {
        std::string const& col = colVal.first;
        T const& val = colVal.second;
        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }


    /// Recursive variadic function (overloaded for column names given as char const*)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string& sql,
                     std::pair<char const*,T> colVal,
                     Targs... Fargs) const {
        std::string const  col = colVal.first;
        T const& val = colVal.second;
        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }

    /**
     * Pack pairs of column names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     * @code
     *   UPDATE <table> SET <packed-pairs>
     * @code
     * @note The method allows any number of arguments and any types of values.
     * @note  Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * @note The column names will be surrounded with back-tick quotes.
     *
     * For example, the following call:
     * @code
     *     sqlPackPairs (
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123),
     *         std::make_pair("fk_id", Function::LAST_INSERT_ID));
     * @code
     * will produce the following string content:
     * @code
     *     `col1`='st\'r',`col2`="c",`col3`=123,`fk_id`=LAST_INSERT_ID()
     * @code
     *
     * @param Fargs the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlPackPairs(Targs... Fargs) const {
        std::string sql;
        sqlPackPair(sql, Fargs...);
        return sql;
    }

    /**
     * The generator of identifiers
     *
     * @note The column name will be surrounded by back ticks.
     * @note Values of string types will be escaped and surrounded by single quotes.
     *
     * @param col the name of a column
     * @param values an iterable collection of values
     * @return `col` IN (<val1>,<val2>,<val3>,,,)
     */
    template <typename T>
    std::string sqlIn(std::string const& col,
                      T const& values) const {
        std::ostringstream ss;
        ss << sqlId(col) << " IN (";
        int num=0;
        for (auto&& val: values)
            ss << (num++ ? "," : "") << sqlValue(val);
        ss << ")";
        return ss.str();
    }

    /**
     * Generate an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     * @code
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     * @code
     * @note  The method allows any number of arguments and any types of value types.
     * @note  Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * @note  The column names will be surrounded with back-tick quotes.
     *
     * For example:
     * @code
     *     connection->sqlSimpleUpdateQuery (
     *         "table",
     *         sqlEqual("fk_id", Function::LAST_INSERT_ID),
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123));
     * @code
     * This will generate the following query (extra newline symbols are added
     * to me this example a bit easy to read:
     * @code
     *     UPDATE `table`
     *     SET `col1`='st\'r',
     *         `col2`="c",
     *         `col3`=123
     *     WHERE
     *       `fk_id`=LAST_INSERT_ID()
     * @code
     *
     * @param tableName the name of a table
     * @param condition the optional condition for selecting rows to be updated
     * @param Fargs the variadic list of values to be inserted
     * @return well-formed SQL statement
     */
    template <typename...Targs>
    std::string sqlSimpleUpdateQuery(std::string const& tableName,
                                     std::string const& condition,
                                     Targs... Fargs) const {
        std::ostringstream qs;
        qs  << "UPDATE " << sqlId(tableName)       << " "
            << "SET "    << sqlPackPairs(Fargs...) << " "
            << (condition.empty() ? "" : "WHERE " + condition);
        return qs.str();
    }

    ///  @return the status of the transaction
    bool inTransaction() const { return _inTransaction; }

    /**
     * Start a transaction
     *
     * @return a smart pointer to self to allow chained calls
     * @throw std::logic_error if the transaction was already been started
     * @throw Error for any other MySQL specific errors
     */
    Connection::Ptr begin();

    /**
     * Commit the current transaction
     *
     * @return a smart pointer to self to allow chained calls
     * @throw std::logic_error if the transaction was not started
     * @throw Error for any other MySQL specific errors
     */
    Connection::Ptr commit();

    /**
     * Rollback the current transaction
     *
     * @return smart pointer to self to allow chained calls
     * @throw std::logic_error if the transaction was not started
     * @throw Error for any other MySQL specific errors
     */
    Connection::Ptr rollback();

    /**
     * Execute the specified query and initialize object context to allow
     * a result set extraction.
     *
     * @param query query to be executed
     * @return a smart pointer to self to allow chained calls
     * 
     * @throw std::invalid_argument for empty query strings
     * @throw ER_DUP_ENTRY_ for attempts to insert rows with duplicate keys
     * @throw Error for any other MySQL specific errors
     */
    Connection::Ptr execute(std::string const& query);

    /**
     * Execute an SQL statement for inserting a new row into a table based
     * on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. Arguments of
     * types 'std::string' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * The effect:
     * @code
     *   INSERT INTO <table> VALUES (<packed-values>)
     * @code
     * @note The method will *NOT* start a transaction, neither it will
     *   commit the one in the end. Transaction management is a responsibility
     *   of a caller of the method.
     *
     * @see Connection::sqlInsertQuery()
     *
     * @param tableName the name of a table
     * @param Fargs the variadic list of values to be inserted
     * @return a smart pointer to self to allow chained calls
     *
     * @throw ER_DUP_ENTRY_ for attempts to insert rows with duplicate keys
     * @throw Error for any other MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeInsertQuery(std::string const& tableName,
                                       Targs... Fargs) {
        return execute(sqlInsertQuery(tableName, Fargs...));
    }

    /**
     * Execute an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     * The effect:
     * @code
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     * @code
     *
     * @note The method will *NOT* start a transaction, neither it will
     *   commit the one in the end. Transaction management is a responsibility
     *   of a caller of the method.
     * @see Connection::sqlSimpleUpdateQuery()
     *
     * @param tableName the name of a table
     * @param whereCondition the optional condition for selecting rows to be updated
     * @param Fargs the variadic list of column-value pairs to be updated
     *
     * @return a smart pointer to self to allow chained calls
     *
     * @throw std::invalid_argument for empty query strings
     * @throw Error for any MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeSimpleUpdateQuery(std::string const& tableName,
                                             std::string const& condition,
                                             Targs... Fargs) {
        return execute(sqlSimpleUpdateQuery(tableName,
                                            condition,
                                            Fargs...));
    }

    /**
     * Execute a user-supplied algorithm which could be retried the specified
     * number of times (or until a given timeout expires) if a connection to
     * a server is lost and re-established before the completion of the algorithm.
     * The number of allowed auto-reconnects and the timeout are controlled by
     * the corresponding parameters of the method.
     *
     * @note In case of reconnects and retries the failed transaction will be aborted.
     * @note It's up to a user script to begin and commit a transaction as needed.
     * @note It's up to a user script to take care of side effects if the script will run
     *   more than once.
     *
     * Example:
     * @code
     *     Configuration::setDatabaseConnectTimeoutSec(60);
     *     ConnectionParams params = ... ;
     *     Connection::Ptr conn = Connection::open(params);
     *     try {
     *         conn->execute(
     *             [](Connection::Ptr conn) {
     *                 conn->begin();
     *                 conn->execute("SELECT ...");
     *                 conn->execute("INSERT ...");
     *                 conn->commit();
     *             },
     *             10                        // 10 extra attempts before to fail
     *             2 * connectionTimeoutSec  // no longer that 120 seconds
     *         );
     *     } catch (ConnectError const& ex) {
     *         cerr << "you only made one failed attempt because "
     *              << "no automatic reconnects were allowed. Open your connection "
     *              << "with factory method Connection::openWait()" << endl;
     *     } catch (ConnectTimeoutError const& ex) {
     *         cerr << "you have exausted the maximum allowed number of retries "
     *              << "within the specified (or implicitly assumed) "
     *                 "timeout: " << ex.timeoutSec() << endl;
     *     } catch (Error const& ex) {
     *         cerr << "failed due to an unrecoverable error: " << ex.what() 
     *     }
     * @code
     *
     * @param script a user-provided function (the callable) to execute
     * @param maxReconnects (optional) maximum number of reconnects allowed
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseMaxReconnects().
     * @param timeoutSec (optional) the maximum duration of time allowed for
     *   the procedure to wait before a connection will be established.
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseConnectTimeoutSec().
     *
     * @throw std::invalid_argument if 'nullptr' is passed in place of 'script'
     * @throw ConnectError failed to establish a connection if connection was
     *   open with factory method Connection::open().
     * @throw ConnectTimeoutError failed to establish a connection within a timeout
     *   if a connection was open with factory method Connection::openWait().
     *
     * @throw MaxReconnectsExceeded for multiple failed attempts (due to connection losses
     *   and subsequent reconnects) to execute the user function. And the number of the attempts
     *   exceeded a limit set by parameter 'maxReconnects'.
     * 
     * @throw Error the general exception for any MySQL specific errors. A client code may also
     *   catch specific subclasses (other than ConnectError or ConnectTimeoutError) of that class
     *   if needed.
     *
     * @return a pointer to the same connector against which the method was invoked
     *   in case of successful completion of the requested operaton.
     *
     * @see Configuration::databaseMaxReconnects()
     * @see Configuration::setDatabaseMaxReconnects()
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Configuration::setDatabaseConnectTimeoutSec()
     * @see Connection::open()
     */
    Connection::Ptr execute(std::function<void(Ptr)> const& script,
                            unsigned int maxReconnects=0,
                            unsigned int timeoutSec=0);

    /**
     * Execute a script within its own transaction to be automatically started and commited.
     *
     * @note upon a completion of the method, depending of an outcome, the transaction
     *   is guaranteed to be commited or aborted.
     * @param script user-provided function (the callable) to execute
     * @param maxReconnects
     *   (optional) maximum number of reconnects allowed
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseMaxReconnects().
     * @param timeoutSec
     *   (optional) the maximum duration of time allowed for
     *   the procedure to wait before a connection will be established.
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseConnectTimeoutSec().
     * @param maxRetriesOnDeadLock (optional) the number of retries on exception
     *   LockDeadlock that may happen while two or many threads/processes were
     *   trying to update the same table.
     * @see Connection::execute()
     */
    Connection::Ptr executeInOwnTransaction(std::function<void(Ptr)> const& script,
                                            unsigned int maxReconnects=0,
                                            unsigned int timeoutSec=0,
                                            unsigned int maxRetriesOnDeadLock=0);

    /**
     * This is just a convenience method for a typical use case.
     * @note both scripts are executed in own transactions. No transaction cleanup
     *   will be needed upon completion of the method.
     * @param insertScript user-provided function (the callable) to be executed first
     *   to insert new data into a database.
     * @param updateScript user-provided function (the callable) to be executed as
     *   as the failover solution to update existing data in the database if the first
     *   script fails due to the ER_DUP_ENTRY_ exception. 
     * @param maxReconnects
     *   (optional) maximum number of reconnects allowed
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseMaxReconnects().
     * @param timeoutSec
     *   (optional) the maximum duration of time allowed for
     *   the procedure to wait before a connection will be established.
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseConnectTimeoutSec().
     * @param maxRetriesOnDeadLock (optional) the number of retries on exception
     *   LockDeadlock that may happen while two or many threads/processes were
     *   trying to update the same table.
     */
    Connection::Ptr executeInsertOrUpdate(std::function<void(Ptr)> const& insertScript,
                                          std::function<void(Ptr)> const& updateScript,
                                          unsigned int maxReconnects=0,
                                          unsigned int timeoutSec=0,
                                          unsigned int maxRetriesOnDeadLock=0);

    /**
     * @return 'true' if the last successful query returned a result set
     *   (even though it may be empty).
     */
    bool hasResult() const;

    /**
     * @note Columns are returned exactly in the same order they were
     *   requested in the corresponding query.
     * @return names of the columns from the current result set
     * @throw std::logic_error if no SQL statement has ever been executed, or
     *   if the last query failed.
     */
    std::vector<std::string> const& columnNames() const;

    /**
     * @return the number of columns in the current result set
     * @throw std::logic_error if no SQL statement has ever been executed, or
     *   if the last query failed.
     */
    size_t numFields() const;

    /**
     * Fill a Protobuf object representing a field
     * 
     * @note The method can be called only upon a successful completion of a query
     *   which has a result set. Otherwise it will throw an exception.
     *
     * @see mysql_fetch_field()
     * @see database::mysql::Connection::hasResult
     *
     * @param ptr a pointer to the Protobuf object to be populated
     * @param idx a relative (0 based) index of the field in a result set
     *
     * @throw std::logic_error if no SQL statement has ever been executed, or
     *   if the last query failed.
     * @throw std::out_of_range if the specified index exceed the maximum index of a result set.
     */
    void exportField(ProtocolResponseSqlField* ptr, size_t idx) const;

    /**
     * Move the iterator to the next (first) row of the current result set
     * and if the iterator is not beyond the last row then initialize an object
     * passed as a parameter.
     *
     * @note Objects initialized upon the successful completion
     *   of the method are valid until the next call to the method or before
     *   the next query. Hence the safe practice for using this method to iterate
     *   over a result set would be:
     * @code
     *     Connection::Ptr conn = Connection::connect(...);
     *     conn->execute ("SELECT ...");
     *
     *     Row row;
     *     while (conn->next(row)) {
     *         // Extract data from 'row' within this block
     *         // before proceeding to the next row, etc.
     *     }
     * @code
     *
     * @return 'true' if the row was initialized or 'false' if past the last row
     *   in the result set.
     *
     * @throw std::logic_error if no SQL statement has ever been executed, or
     *    if the last query failed.
     */
    bool next(Row& row);

    /**
     * The convenience method is for executing a query from which a single value
     * will be extracted (typically a PK). Please, read the notes below.
     *
     * @note By default the method requires a result set to have 0 or 1 rows.
     *   If the result set has more than one row exception std::logic_error
     *   will be thrown.
     * @note The previously mentioned requirement can be relaxed by setting
     *   a value of the optional parameter 'noMoreThanOne' to 'false'.
     *   In that case a value from the very first row will be extracted.
     * @note If a result set is empty the method will throw EmptyResultSetError
     * @note If the field has 'NULL' the method will return 'false'
     * @note If the conversion to a proposed type will fail the method will
     *    throw InvalidTypeError
     *
     * @param query a query to be executed
     * @param col the name of a column from which to extract a value
     * @param val a value to be set (unless the field contains NULL)
     * @param noMoreThanOne a flag (if set) forcing the above explained behavior
     * @return 'true' if the value is not NULL
     */
    template <typename T>
    bool executeSingleValueSelect(std::string const& query,
                                  std::string const& col,
                                  T& val,
                                  bool noMoreThanOne=true) {
        std::string const context_ = "DatabaseMySQL::" + std::string(__func__) + "  ";
        execute(query);
        if (!hasResult()) throw EmptyResultSetError(context_ + "result set is empty");

        bool isNotNull = false;
        size_t numRows = 0;
        Row row;
        while (next(row)) {
            // Only the very first row matters
            if (!numRows) isNotNull = row.get(col, val);

            // Have to read the rest of the result set to avoid problems with the MySQL protocol
            ++numRows;
        }
        if ((1 == numRows) or !noMoreThanOne) return isNotNull;
        throw std::logic_error(context_ + "result set has more than 1 row");
    }

    /**
     * Retreive the number of warnings generated by the last query in the current session.
     *
     * @note Calling this method won't reset warnings, which can be retrieved later
     *   by calling method warnings(). Calling this method many times in a row will return
     *   the same result.
     * @see Connection::warnings
     *
     * @return A value of the counter (0 or higher).
     */
    unsigned int warningCount();

    /**
     * Retrieve warning, errors or notes generated after executing the last statement.
     * This operation is implemented as MySQL query "SHOW WARNINGS".
     *
     * @note The operation will not reset the query statistics.  Calling this method
     *   many times in a row will return the same result.
     * @see Connection::warningCount
     *
     * @return A collection of entries.
     */
    std::list<Warning> warnings();

private:

    /**
     * @see Connection::open()
     * @see Connection::open2()
     */
    Connection(ConnectionParams const& connectionParams,
               unsigned int connectTimeoutSec);

    /**
     * Keep trying to connect to a server until either a timeout expires, or
     * some unrecoverable failure happens while trying to establish a connection.
     *
     * @throw ConnectTimeoutError if failed to establish a connection within a timeout
     * @throw Error for other problems when preparing or establishing a connection
     */
    void _connect();

    /**
     * Make exactly one attempt to establish a connection
     *
     * @throw ConnectError connection to a server failed
     * @throw Error other problem when preparing or establishing a connection
     */
    void _connectOnce();

    /**
     * The method is called to process the last error, reconnect if needed (and allowed), etc.
     *
     * @param context a context from which this method was called
     * @param instantAutoReconnect an optional flag allowing to make instant reconnects for
     *   qualified error conditions
     *
     * @throw std::logic_error if the method is called after no actual error happened
     * @throw Reconnected after a successful reconnection has happened
     * @throw ConnectError connection to a server failed
     * @throw ER_DUP_ENTRY_ after the last statement attempted to violate
     *   the corresponding key constraint
     * @throw Error for some other error not listed above
     */
    void _processLastError(std::string const& context,
                           bool instantAutoReconnect=true);

    /**
     * The method is to ensure that the transaction is in the desired state.
     * @param inTransaction the desired state of the transaction
     */
    void _assertTransaction(bool inTransaction) const;

    /**
     * The method is to ensure that a proper query context is set and
     * its result set can be explored.
     * @throw Error if the connection is not established or no prior query was made
     */
    void _assertQueryContext() const;

    static std::atomic<size_t> _nextId;         // The next available connector identifier in the generator's sequence
    size_t const _id;                           // Unique identifier of this connector
    ConnectionParams const _connectionParams;   // Parameters of the connection
    unsigned int _connectTimeoutSec;            // Time to wait between reconnection attempts
    std::string _lastQuery;                     // The last SQL statement
    bool _inTransaction;                        // Transaction status
    MYSQL* _mysql;                              // MySQL connection
    unsigned long _mysqlThreadId;               // Thread ID of the current connection
    unsigned long _connectionAttempt;           // The counter of attempts between successful reconnects

    // A result set of the last successful query

    MYSQL_RES* _res;
    MYSQL_FIELD* _fields;
    size_t _numFields;
    std::vector<std::string> _columnNames;
    std::map<std::string, size_t> _name2index;

    std::string _charSetName;   // of the current connection

    // The row object gets updated after fetching each row of the result set.
    // It's required to be cached here to ensure at least the same lifespan as
    // the one of thw current class while a client is processing the last result set.
    MYSQL_ROW _row;
};


/**
 * Class ConnectionPool manages a pool of the similarly configured persistent
 * database connection. The number of connections is determined by the corresponding
 * Configuration parameter. Connections will be added to the pool (up to that limit)
 * on demand. This ensures that the class constructor is not blocking in case if
 * (or while) the corresponding MySQL/MariaDB service is not responding.
 * 
 * @note This class is meant to be used indirectly by passing its instances
 *   to the constructor of class ConnectionHandler.
 * @see class ConnectionHandler
 */
class ConnectionPool {
public:
    typedef std::shared_ptr<ConnectionPool> Ptr;

    /**
     * The static factory object creates a pool and sets the maximum number
     * of connections.
     * 
     * @note this is a non-blocking method. In particular, No connection attempts
     *   will be made by this method.
     *
     * @param params  parameters of the connections
     * @param maxConnections  the maximum number of connections managed by the pool
     * @return smart pointer to the newly created object
     */
    static Ptr create(ConnectionParams const& params,
                      size_t maxConnections);

    ConnectionPool() = delete;
    ConnectionPool(ConnectionPool const&) = delete;
    ConnectionPool& operator=(ConnectionPool const&) = delete;

    /**
     * Allocate (and open a new if required/possible) connection
     *
     * @note the requester must return the service back after it's no longer needed.
     * @return pointer to the allocated connector
     * @see ConnectionPool::release()
     */
    Connection::Ptr allocate();

    /**
     * Return a connection object back into the pool of the available ones.
     *
     * @param conn  connection object to be returned back
     * @throw std::logic_error if the object was not previously allocated
     * @see ConnectionPool::allocate()
     */
    void release(Connection::Ptr const& conn);

private:
   /// @see ConnectionPool::create
    ConnectionPool(ConnectionParams const& params,
                   size_t maxConnections);

    // Input parameters

    ConnectionParams const _params;
    size_t const _maxConnections;

    /// Connection objects which are available
    std::list<Connection::Ptr> _availableConnections;

    /// Connection objects which are in use
    std::list<Connection::Ptr> _usedConnections;

    /// The mutex for enforcing thread safety of the class's public API and
    /// internal operations. The mutex is locked by methods ConnectionPool::allocate()
    /// and ConnectionPool::release() when moving connection objects between
    /// the lists (defined above).
     mutable std::mutex _mtx;

    /// The condition variable for notifying client threads waiting for the next
    /// available connection.
    std::condition_variable _available;
};


/**
 * Class ConnectionHandler implements the RAII method of handling database
 * connection.
 */
class ConnectionHandler {
public:
    /**
     * The default constructor will initialize the connection pointer
     * with 'nullptr'
     */
    ConnectionHandler() = default;

    /**
     * Construct with a connection
     *
     * @param conn_  connection to be watched and managed
     */
    explicit ConnectionHandler(Connection::Ptr const& conn_);

    /**
     * Construct with a pointer to a connection pool for allocating
     * a connection. The connection will get released by the destructor.
     *
     * @param pool  connection pool to acquire persistent connections
     */
    explicit ConnectionHandler(ConnectionPool::Ptr const& pool);

    ConnectionHandler(ConnectionHandler const&) = delete;
    ConnectionHandler& operator=(ConnectionHandler const&) = delete;

    /**
     * The destructor will rollback a transaction if any was started at
     * a presence of a connection.
     */
    ~ConnectionHandler();

    /// The smart reference to the connector object (if any))
    Connection::Ptr conn;

private:
    /// The smart reference to the connector pool object (if any))
    ConnectionPool::Ptr _pool;
};

}}}}} // namespace lsst::qserv::replica::database::mysql

#endif // LSST_QSERV_REPLICA_DATABASEMYSQL_H
