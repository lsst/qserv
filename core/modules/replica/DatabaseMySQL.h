/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// DatabaseMySQL.h declares:
///
/// class Raw
/// class Connection
/// (see individual class documentation for more information)

// System headers
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include <mysql/mysql.h>

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
class Error
    :   public std::runtime_error {

public:

    /**
     * The normal constructor
     *
     * @param what - reason for the exceptioon
     */
    explicit Error(std::string const& what)
        :   std::runtime_error(what) {
    }
};

/**
 * Instances of this exception class are thrown on attempts to insert
 * rows with duplicate keys.
 */
class DuplicateKeyError
    :   public Error {

public:

    /**
     * The normal constructor
     *
     * @param what - reason for the exceptioon
     */
    explicit DuplicateKeyError(std::string const& what)
        :   Error(what) {
    }
};

/**
 * Instances of this exception class are thrown on failed attempts
 * to interpret the contents of the result set.
 */
class InvalidTypeError
    :   public Error {

public:

    /**
     * The normal constructor
     *
     * @param what - reason for the exceptioon
     */
    explicit InvalidTypeError(std::string const& what)
        :   Error(what) {
    }
};

/**
 * Instances of this exception class are thrown on empty result sets
 * by some methods when a query is supposed to return at least one row.
 */
class EmptyResultSetError
    :   public Error {

public:

    /**
     * The normal constructor
     *
     * @param what - reason for the exceptioon
     */
    explicit EmptyResultSetError(std::string const& what)
        :   Error(what) {
    }
};

/**
 * Struvct ConnectionParams encapsulates connection parameters to a MySQL server
 */
struct ConnectionParams {
    /// The DNS name or IP address of a machine where the database
    /// server runs
    std::string host;

    /// The port number of the MySQL service
    uint16_t port;

    /// The name of a database user
    std::string user;

    /// The database password
    std::string password;

    /// The name of a database to be set upon the connection
    std::string database;

    /**
     * The factory method will return an instance of this structure initialized
     * by values of parameters found in the input encoded string. The string is
     * expected to have the following syntax:
     *
     *   mysql://[user][:password]@[host][:port][/database]
     *
     * NOTES ON THE SYNTAX:
     * 1) all keywords are madatory
     * 2) the corresponding values for for all but the database are optional
     * 3) default values for other parameters (if missing in the string) will be assumed.
     *
     * @param params          - connection parameters packed into a string
     * @param defaultHost     - default value for a host name
     * @param defaultPort     - default port number
     * @param defaultUser     - default value for a database user account
     * @param defaultPassword - default value for a database user account
     *
     * @throw std::invalid_argument - if the string can't be parsed
     */
    static ConnectionParams parse(std::string const& params,
                                  std::string const& defaultHost,
                                  uint16_t defaultPort,
                                  std::string const& defaultUser,
                                  std::string const& defaultPassword);

    /**
     * Return a string representation of all (but the password) parameters.
     * The result will be formatted similarily to the one expected by
     * the non-default constructor of the class.
     */
    std::string toString() const;
};

/// Overloaded operator for serializing ConnectionParams instances
std::ostream& operator<<(std::ostream&, ConnectionParams const&);

/**
 * Class Row represens the current row obtained from the last result set.
 * It provides an interface for obtainig values of fields and translating
 * them from the internal MySQL representation into the proposed C++ type
 * system.
 *
 * All type-specific 'get' methods defined in this class will return 'true' and
 * set the value returned for the specified column if the value was not 'NULL'.
 * They will return 'false' otherwise. All methods have two parameters:
 *
 *   columnName - the name of a column
 *   value      - the value (of a type which depends on the method signature)
 *                to be initialized upon the succesful completion of a method
 *
 * Methods may also throw the following exceptions:
 *
 *   std::invalid_argument - for unknown column names
 *   InvalidTypeError      - when the conversion of row data into a value of
 *                           the requested type is not possible.
 *
 * @note the validity of each object of this class is limited by the life
 * span of the database Connection object and a result set of the last
 * query. Use this object only for short periods of time while iterating over
 * a result set after each successful invocation of the iterator method
 * Connection::next().
 *
 * @see Connection::next()
 *
 */
class Row {

public:

    /// Class Connection is allowed to initialize the valid content of rows
    friend class Connection;

    /**
     * The class encapsulate a raw data pointer and the number of bytes
     * in each column.
     */
    typedef std::pair<char const*, size_t> Cell;

    /**
     * The default constructor will initialize invalid instances of the class.
     *
     * @note any attempts to call most (but 'isValid', copy constructor,
     * assignment operator and desturctor) methods of objects constracted
     * with this state will throw exception std::logic_error. So, make sure
     * the object is properly set by passing it for initialization to method
     * Connection::next() when iterating over a result set.
     *
     * @see Connection::next()
     */
    Row();

    /// Copy constructor
    Row(Row const& rhs) = default;

    /// The Assignment operator
    Row& operator=(Row const& rhs) = default;

    ~Row() = default;

    /// @return 'true' of the object has meaningful content
    bool isValid() const { return _name2indexPtr != nullptr; }

    /// @return width of the row
    size_t numColumns() const;

    // These methods will return 'true' if the specified field is NULL

    bool isNull(size_t             columnIdx)  const;
    bool isNull(std::string const& columnName) const;

    // Type-specific data extractors/converters for values of fields.
    // There are two ways to access the values: either by a relative
    // index of a column in a result set, or by the name of the column.
    // The second method has some extra (though, minor) overhead.
    //
    // @see class Row

    bool get(size_t      columnIdx,         std::string& value) const;
    bool get(std::string const& columnName, std::string& value) const;

    // Unsigned integer types

    bool get(size_t columnIdx, uint64_t& value) const;
    bool get(size_t columnIdx, uint32_t& value) const;
    bool get(size_t columnIdx, uint16_t& value) const;
    bool get(size_t columnIdx, uint8_t&  value) const;

    bool get(std::string const& columnName, uint64_t& value) const;
    bool get(std::string const& columnName, uint32_t& value) const;
    bool get(std::string const& columnName, uint16_t& value) const;
    bool get(std::string const& columnName, uint8_t&  value) const;

    // Signed integer types

    bool get(size_t columnIdx, int64_t& value) const;
    bool get(size_t columnIdx, int32_t& value) const;
    bool get(size_t columnIdx, int16_t& value) const;
    bool get(size_t columnIdx, int8_t&  value) const;

    bool get(std::string const& columnName, int64_t& value) const;
    bool get(std::string const& columnName, int32_t& value) const;
    bool get(std::string const& columnName, int16_t& value) const;
    bool get(std::string const& columnName, int8_t&  value) const;

    // Floating point types

    bool get(size_t columnIdx, float&  value) const;
    bool get(size_t columnIdx, double& value) const;

    bool get(std::string const& columnName, float&  value) const;
    bool get(std::string const& columnName, double& value) const;

    // Other types

    bool get(size_t columnIdx, bool& value) const;

    bool get(std::string const& columnName, bool&  value) const;

    /**
     * @return reference to the data cell for the column
     * @param columnIdx - the index of a column
     */
    Cell const& getDataCell(size_t columnIdx) const;

    /**
     * @return reference to the data cell for the column
     * @param columnName - the name of a column
     */
    Cell const& getDataCell(std::string const& columnName) const;

private:

    /** Mapping column names to the indexes
     *
     * NOTE: if the pointer is set to 'nullptr' then the object is not
     * in the valid state. The valid state is set by class Connection
     * when iterating over a result set.
     */
    std::map<std::string, size_t> const* _name2indexPtr;

    /// Mapping column indexes to the raw data cells
    std::vector<Cell> _index2cell;
};

/**
 * Class DoNotProcess is an abstraction for SQL strings which than ordinary
 * values of string types needs to be injected into SQL statements without
 * being processed (excaped and quoted) as regular string values.
 */
class DoNotProcess {

public:

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
    explicit DoNotProcess(std::string const& name_);

    DoNotProcess() = delete;

    DoNotProcess(DoNotProcess const&) = default;
    DoNotProcess& operator=(DoNotProcess const&) = default;

    virtual ~DoNotProcess() = default;

public:

    /**
     * The exact string value as it should appear within queries. It will
     * be extracted by the corresponding query generators.
     */
    std::string name;
};

/**
 * Class Keyword is an abstraction for SQL keywords which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Keyword
    :   public DoNotProcess {

public:

    // Predefined SQL keywords

    /// @return the object representing the corresponding SQL keyword
    static Keyword const SQL_NULL;

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
    explicit Keyword(std::string const& name_);

    Keyword() = delete;

    Keyword(Keyword const&) = default;
    Keyword& operator=(Keyword const&) = default;

    ~Keyword() override = default;
};

/**
 * Class Function is an abstraction for SQL functions which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Function
    :   public DoNotProcess {

public:

    /// @return the object representing the corresponding SQL function
    static Function const LAST_INSERT_ID;

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
    explicit Function(std::string const& name_);

    Function() = delete;

    Function(Function const&) = default;
    Function& operator=(Function const&) = default;

    ~Function() override = default;
};

/**
 * Class Connection provides the main API to the database.
 */
class Connection
    :   public std::enable_shared_from_this<Connection> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Connection> Ptr;

    /**
     * Connect to the MySQL service with the specified parameters and return
     * a pointer to the Connection object.
     *
     * @param connectionParams - the connection parameters
     * @param autoReconnect    - automatically reconnect to the service
     *                           if the dropped connection was discovered.
     *                           This option is useful when the application is inactive
     *                           for a prologed period of time causing the server to kick out
     *                           the client. Note that only one reconnection attempt will be
     *                           made each time the dropped conneciton is detected.
     * @autoCommit             - if set 'true' then no transactions will be required
     *                           whene executing queries modifying the contents of tables.
     *                           ATTENTION: this is a special option which must be used
     *                           with care. One proposed use case for it is
     *                           for executing 'LOCK/UNLOCK TABLES' statements.
     *
     * @return a valid object if the connection attempt succeeded
     * @throws Error - if the connection failed
     */
    static Ptr open(ConnectionParams const& connectionParams,
                    bool autoReconnect = true,
                    bool autoCommit    = false);

    // Default construction and copy semantics are prohibited

    Connection() = delete;
    Connection(Connection const&) = delete;
    Connection& operator=(Connection const&) = delete;

    ~Connection();

    /**
      * A front-end to mysql_real_escape_string()
      *
      * @param str - a string to be processed
      * @return the processed string
      */
    std::string escape(std::string const& str) const;

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
    template <typename T,
              typename ...Targs>
    void sqlValues(std::string& sql,
                   T            val,
                   Targs...     Fargs) const {

        bool const last = sizeof...(Fargs) - 1 < 0;
        std::ostringstream ss;
        ss << (sql.empty() ? "(" : (last ? "" : ",")) << sqlValue(val);
        sql += ss.str();

        // Recursively keep drilling down the list of arguments with one
        // argument less.
        sqlValues(sql, Fargs...);
    }

    /**
     * Turn values of variadic argumenst into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     *   @code
     *     sqlPackValues ("st'r", std::string("c"), 123, 24.5);
     *   @code
     * will produce the following output:
     *   @code
     *     ('st\'r','c',123,24.5)
     *   @code
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
     * any number of arguments and any types of argument values. rguments of
     * types 'std::string' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * @param tableName - the name of a table
     * @param Fargs     - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlInsertQuery(std::string const& tableName,
                               Targs...           Fargs) const {
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

    /**
     * Return:
     *
     *   `col` = <value>
     *
     * Where:
     * - the column name will be surrounded by back ticks
     * - values of string types will be escped and surrounded by single quotes
     */
    template <typename T>
    std::string sqlEqual(std::string const& col,
                         T const&           val) const {
        std::ostringstream ss;
        ss << sqlId(col) << "=" << sqlValue(val);
        return ss.str();
    }

    /// The base (the final function) to be called
    void sqlPackPair(std::string&) const {}

    /// Recursive variadic function (overloaded for column names gived as std::string)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string&             sql,
                     std::pair<std::string,T> colVal,
                     Targs...                 Fargs) const {

        std::string const& col = colVal.first;
        T const&           val = colVal.second;

        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }


    /// Recursive variadic function (overloaded for column names gived as char const*)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string&             sql,
                     std::pair<char const*,T> colVal,
                     Targs...                 Fargs) const {

        std::string const  col = colVal.first;
        T const&           val = colVal.second;

        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }

    /**
     * Pack pairs of column names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     *
     *   UPDATE <table> SET <packed-pairs>
     *
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * For example, the following call:
     * @code
     *     sqlPackPairs (
     *         std::make_pair ("col1",  "st'r"),
     *         std::make_pair ("col2",  std::string("c")),
     *         std::make_pair ("col3",  123),
     *         std::make_pair ("fk_id", Function::LAST_INSERT_ID));
     * @code
     * will produce the following string content:
     * @code
     *     `col1`='st\'r',`col2`="c",`col3`=123,`fk_id`=LAST_INSERT_ID()
     * @code
     *
     * @param Fargs          - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlPackPairs(Targs... Fargs) const {
        std::string sql;
        sqlPackPair(sql, Fargs...);
        return sql;
    }

    /**
     * Return:
     *
     *   `col` IN (<val1>,<val2>,<val3>,,,)
     *
     * NOTES:
     * - the column name will be surrounded by back ticks
     * - values of string types will be escaped and surrounded by single quotes
     *
     * @param col    - the name of a column
     * @param values - an iterable collection of values
     */
    template <typename T>
    std::string sqlIn(std::string const& col,
                      T const&           values) const {
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
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     *
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * For example:
     * @code
     *     connection->sqlSimpleUpdateQuery (
     *         "table",
     *         sqlEqual("fk_id", Function::LAST_INSERT_ID),
     *         std::make_pair ("col1",  "st'r"),
     *         std::make_pair ("col2",  std::string("c")),
     *         std::make_pair ("col3",  123));
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
     * @param tableName - the name of a table
     * @param condition - the optional condition for selecting rows to be updated
     * @param Fargs     - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlSimpleUpdateQuery(std::string const& tableName,
                                     std::string const& condition,
                                     Targs...           Fargs) const {
        std::ostringstream qs;
        qs  << "UPDATE " << sqlId(tableName)       << " "
            << "SET "    << sqlPackPairs(Fargs...) << " "
            << (condition.empty() ? "" : "WHERE " + condition);
        return qs.str();
    }

    /**
     * Return the status of the transaction
     */
    bool inTransaction() const { return _inTransaction; }

    /**
     * Start the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was already been started
     */
    Connection::Ptr begin();

    /**
     * Commit the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was not started
     */
    Connection::Ptr commit();

    /**
     * Rollback the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was not started
     */
    Connection::Ptr rollback();

    /**
     * Execute the specified query and initialize object context to allow
     * a result set extraction.
     *
     * @param  query                 - a query to be execured
     * @return                       - the smart pointer to self to allow chaned calles.
     * @throws std::invalid_argument - for empty query strings
     * @throws DuplicateKeyError     - for attempts to insert rows with duplicate keys
     * @throws Error                 - for any other MySQL specific errors
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
     *
     *   INSERT INTO <table> VALUES (<packed-values>)
     *
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of the method.
     *
     * @see Connection::sqlInsertQuery()
     *
     * @param tableName - the name of a table
     * @param Fargs     - the variadic list of values to be inserted
     *
     * @return - the smart pointer to self to allow chaned calles.
     *
     * @throws DuplicateKeyError     - for attempts to insert rows with duplicate keys
     * @throws Error                 - for any other MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeInsertQuery(std::string const& tableName,
                                       Targs...           Fargs) {

        return execute(sqlInsertQuery(tableName,
                                      Fargs...));
    }

    /**
     * Execute an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     * The effect:
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     *
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of the method.
     *
     * @see Connection::sqlSimpleUpdateQuery()
     *
     * @param tableName      - the name of a table
     * @param whereCondition - the optional condition for selecting rows to be updated
     * @param Fargs          - the variadic list of column-value pairs to be updated
     *
     * @return - the smart pointer to self to allow chaned calles.
     *
     * @throws std::invalid_argument - for empty query strings
     * @throws Error                 - for any MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeSimpleUpdateQuery(std::string const& tableName,
                                             std::string const& condition,
                                             Targs...           Fargs) {

        return execute(sqlSimpleUpdateQuery(tableName,
                                            condition,
                                            Fargs...));
    }

    /**
     * Returns 'true' if the last successfull query returned a result set
     * (even though it may be empty)
     */
    bool hasResult() const;

    /**
     * Return the names of the columns from the current result set.
     *
     * NOTE: the columns are returned exactly in the same order they were
     *       requested in the corresponding query.
     *
     * @throws std::logic_error - if no SQL statement has ever been executed, or
     *                            if the last query failed.
     */
    std::vector<std::string> const& columnNames() const;

    /**
     * Move the iterator to the next (first) row of the current result set
     * and if the iterator is not beyond the last row then nnitialize an object
     * passed as a parameter.
     *
     * ATTENTION: objects initialized upon the successful completion
     * of the method are valid until the next call to the method or before
     * the next query. Hence the safe practice for using this method to iterate
     * over a result set would be:
     *
     *   @code
     *     Connection::Ptr conn = Connection::connect(...);
     *     conn->execute ("SELECT ...");
     *
     *     Row row;
     *     while (conn->next(row)) {
     *         // Extract data from 'row' within this block
     *         // before proceeding to the next row, etc.
     *     }
     *   @code
     *
     * @throws std::logic_error - if no SQL statement has ever been executed, or
     *                            if the last query failed.
     *
     * @return 'true' if the row was initialized or 'false' if past the last row
     *          in the result set.
     */
    bool next(Row& row);

    /**
     * The convenience method is for executing a query from which a single value
     * will be extracted (typically a PK). Please, read the notes below.
     *
     * NOTES:
     * - by default the method requires a result set to have 0 or 1 rows.
     *   If the result set has more than one row exception std::logic_error
     *   will be thrown.
     *
     * - the previously mentioned requirement can be relaxed by setting
     *   a value of the optional parameter 'noMoreThanOne' to 'false'.
     *   In that case a value from the very first row will be extracted.
     *
     * - if a result set is empty the method will throw EmptyResultSetError
     *
     * - if the field has 'NULL' the method will return 'false'
     *
     * - if the conversion to a proposed type will fail the method will
     *   throw InvalidTypeError
     *
     * @param query - a query to be executed
     * @param col   - the name of a columnt from which to exctract a value
     * @param val   - a value to be set (unless the field contains NULL)
     * @param noMoreThanOne - flag (if set) forcing the above explained behavior
     * @return 'true' if the value is not NULL
     */
    template <typename T>
    bool executeSingleValueSelect(std::string const& query,
                                  std::string const& col,
                                  T&                 val,
                                  bool               noMoreThanOne=true) {
        execute(query);
        if (not hasResult()) {
            throw EmptyResultSetError(
                "DatabaseMySQL::executeSingleValueSelect()  result set is empty");
        }

        bool isNotNull;
        size_t numRows = 0;

        Row row;
        while (next(row)) {

            // Only the very first row matters
            if (not numRows) isNotNull = row.get (col, val);

            // have to read the rest of the result set to avoid problems with the MySQL
            // protocol
            ++numRows;
        }
        if ((1 == numRows) or not noMoreThanOne) return isNotNull;

        throw std::logic_error(
                "DatabaseMySQL::executeSingleValueSelect()  result set has more than 1 row");
    }

private:

    /**
     * Construct an object
     *
     * @see Connection::connect()
     */
    Connection(ConnectionParams const& connectionParams,
               bool autoReconnect,
               bool autoCommit);

    /**
     * Establish a connection
     *
     * @throws Error - if the connection is not possible
     */
    void connect();

    /**
     * The method is to ensure that the transaction is in the desired state.
     *
     * @param inTransaction - the desired state of the transaction
     */
    void assertTransaction(bool inTransaction) const;

    /**
     * The method is to ensure that a proper query context is set and
     * its result set can be explored.
     *
     * @throw Error - if the connection is not established or no prior query was made
     */
    void assertQueryContext() const;

private:

    /// Parameters of the connection
    ConnectionParams const _connectionParams;

    /// Auto-reconnect policy
    bool _autoReconnect;

    /// Auto-commit policy
    bool _autoCommit;

    /// The last SQL statement
    std::string _lastQuery;

    /// Transaction status
    bool _inTransaction;

    /// Connection
    MYSQL* _mysql;

    // Last result set

    MYSQL_RES*   _res;
    MYSQL_FIELD* _fields;

    size_t _numFields;

    std::vector<std::string> _columnNames;

    std::map<std::string, size_t> _name2index;

    // Get updated after fetching each row of the result set

    MYSQL_ROW _row;     // must be cahed here to ensure its lifespan
                        // while a client will be processing its content.
};

}}}}} // namespace lsst::qserv::replica::database::mysql

#endif // LSST_QSERV_REPLICA_DATABASEMYSQL_H
