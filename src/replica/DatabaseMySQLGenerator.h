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
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQLGENERATOR_H
#define LSST_QSERV_REPLICA_DATABASEMYSQLGENERATOR_H

// System headers
#include <memory>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/Csv.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class Connection;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica::database::mysql {

/**
 * Class DoNotProcess is an abstraction for SQL strings which than ordinary
 * values of string types needs to be injected into SQL statements without
 * being processed (escaped and quoted) as regular string values.
 */
class DoNotProcess {
public:
    /// @param str_ the input string
    explicit DoNotProcess(std::string const& str_) : str(str_) {}

    DoNotProcess() = delete;
    DoNotProcess(DoNotProcess const&) = default;
    DoNotProcess& operator=(DoNotProcess const&) = default;

    virtual ~DoNotProcess() = default;

    /// Unmodified value of an input string passed into the normal constructor of
    /// the class. The value would be inserted into queries by query generators.
    std::string str;
};

/// This is an abstraction for pre-processed SQL identifiers.
using SqlId = DoNotProcess;

/**
 * Class Keyword is an abstraction for SQL keywords which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Keyword : public DoNotProcess {
public:
    /// @return the object representing the SQL keyword 'NULL'
    static Keyword const SQL_NULL;

    /// @param str_ the input string
    explicit Keyword(std::string const& str_) : DoNotProcess(str_) {}

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
class Function : public DoNotProcess {
public:
    /// @return an object representing the corresponding SQL function
    static Function const LAST_INSERT_ID;

    /// @return an object representing the row counter selector
    static Function const COUNT_STAR;

    /// @return an object representing the row selector
    static Function const STAR;

    /// @return an object representing the current database selector
    static Function const DATABASE;

    /// @return an object representing the current time selector
    static Function const NOW;

    /// @param column Preprocessed identifier of a column to be selected. The identifier
    ///  is expected to be made using sqlId(columnName).
    /// @return an object representing the function "UNIX_TIMESTAMP(<column>)"
    static Function UNIX_TIMESTAMP(SqlId const& column);

    /// @note Identifiers of teh columns are expected to be formed using calls
    ///   to sqlId(columnName).
    /// @param resolution The resolution of the result: "HOUR", "MINUTE", "SECOND", etc.
    ///   See MySQL documentation for further details.
    /// @param lhs Preprocessed identifier of the left column to be selected.
    /// @param rhs Preprocessed identifier of the left column to be selected.
    static Function TIMESTAMPDIFF(std::string const& resolution, SqlId const& lhs, SqlId const& rhs);
    /// @param str_ the input string
    explicit Function(std::string const& str_) : DoNotProcess(str_) {}

    Function() = delete;
    Function(Function const&) = default;
    Function& operator=(Function const&) = default;

    ~Function() override = default;
};

/**
 * @brief Class QueryGenerator provides the API that facilitates generating MySQL queries.
 *
 * The class design allows using it in one of the three contexts:
 *
 * 1. As a subclass of the class Connection where the query generation methods are
 * injected into the interface of the derived class. In addition, the deriving class
 * should also override the virtual method QueryGenerator::escape() for redirecting
 * the operation to the corresponding MySQL function.
 *
 * 2. As a stand-alone class constructed with an existing connector object. This technique
 * would simplify using the generator in the user code. For example:
 * @code
 *   std::shared_ptr<Connection> conn = ...;
 *   QueryGenerator const g(conn);
 *   std::string const query = g.sqlInsert("constants", Function::LAST_INSERT_ID, "pi", 3.14159);
 * @endcode
 * An alternative approach would be to obtain such generator from the connector object:
 * @code
 *   std::shared_ptr<Connection> conn = ...;
 *   QueryGenerator const g = conn->queryGenerator());
 * @endcode
 *
 * 3. As a stand-alone class constructed without any connector. Objects constructed in this
 * way are meant to be used for unit tests. For example:
 * @code
 *   QueryGenerator const g;
 *   std::string const query = g.sqlInsert("constants", Function::LAST_INSERT_ID, "pi", 3.14159);
 * @endcode
 * @note If the generator is constructed using the last technique no escape processing
 *   will be done by the generator.
 */
class QueryGenerator {
public:
    /**
     * @brief Construct a new Query Generator object.
     * @param conn The optional connector.
     */
    QueryGenerator(std::shared_ptr<Connection> conn = nullptr);

    QueryGenerator(QueryGenerator const&) = default;
    QueryGenerator& operator=(QueryGenerator const&) = default;
    virtual ~QueryGenerator() = default;

    /**
     * @brief The optional string processing algorithm that's meant to be replaced
     *   by te real one in a subclass.
     * @param str The input string to be processed.
     * @return The result string ready to be used in the MySQL queries.
     */
    virtual std::string escape(std::string const& str) const;

    /// @return a non-escaped and back-tick-quoted SQL identifier
    SqlId sqlId(std::string const& str) const { return SqlId("`" + str + "`"); }

    /// @return a composite identifier for a database and a table, or a table and a column
    SqlId sqlId(std::string const& first, std::string const& second) const {
        return SqlId(sqlId(first).str + "." + sqlId(second).str);
    }

    /// @return the input as is
    SqlId sqlId(SqlId const& id) const { return id; }

    /// @return special selector for generating code like: `table`.*
    SqlId sqlId(std::string const& first, Function const& second) const {
        return SqlId(sqlId(first).str + "." + second.str);
    }

    /// @return a back-ticked identifier of a MySQL partition for the given "super-transaction"
    SqlId sqlPartitionId(TransactionId transactionId) const {
        return sqlId("p" + std::to_string(transactionId));
    }

    template <typename TYPEID>
    DoNotProcess sqlDistinctId(TYPEID const& nameOrId) const {
        return DoNotProcess("DISTINCT " + sqlId(nameOrId).str);
    }

    // Type-specific value generators

    template <typename T>
    DoNotProcess sqlValue(T const& v) const {
        return DoNotProcess(std::to_string(v));
    }
    DoNotProcess sqlValue(bool const& v) const { return sqlValue(v ? 1 : 0); }
    DoNotProcess sqlValue(std::string const& str) const { return DoNotProcess("'" + escape(str) + "'"); }
    DoNotProcess sqlValue(char const* str) const { return sqlValue(std::string(str)); }
    DoNotProcess sqlValue(DoNotProcess const& v) const { return v; }
    DoNotProcess sqlValue(Keyword const& k) const { return k; }
    DoNotProcess sqlValue(Function const& f) const { return f; }
    DoNotProcess sqlValue(std::vector<std::string> const& coll) const;

    /**
     * The function replaces the "conditional operator" of C++ in SQL statements
     * generators. Unlike the standard operator this function allows internal
     * type switching while producing a result of a specific type.
     *
     * @return an object which doesn't require any further processing
     */
    DoNotProcess nullIfEmpty(std::string const& str) const {
        if (str.empty()) return Keyword::SQL_NULL;
        return sqlValue(str);
    }

    // Generator: ([value [, value [, ... ]]])
    // Where values of the string types will be surrounded with single quotes

    /// The end of variadic recursion
    void sqlPackValue(std::string& sql) const {}

    /// The next step in the variadic recursion when at least one value is
    /// still available
    template <typename T, typename... Targs>
    void sqlPackValue(std::string& sql, T v, Targs... Fargs) const {
        if (!sql.empty()) sql += ",";
        sql += sqlValue(v).str;
        // Recursively keep drilling down the list of arguments with one argument less.
        sqlPackValue(sql, Fargs...);
    }

    /**
     * Turn values of variadic arguments into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     * @code
     *   sqlPackValues"st'r", std::string("c"), 123, 24.5;
     * @endcode
     * This will produce the following output:
     * @code
     *   "'st\'r','c',123,24.5"
     * @endcode
     */
    template <typename... Targs>
    std::string sqlPackValues(Targs... Fargs) const {
        std::string sql;
        sqlPackValue(sql, Fargs...);
        return sql;
    }

    /// @return A string that's ready to be included into the queries.
    template <typename... Targs>
    std::string sqlValues(Targs... Fargs) const {
        return " VALUES (" + sqlPackValues(Fargs...) + ")";
    }

    /// @return A sub-query object that requires no further processing.
    DoNotProcess sqlSubQuery(std::string const& subQuery) const { return DoNotProcess("(" + subQuery + ")"); }

    // Helper functions for the corresponding functions of the class Function
    // would translate strings into the properly quoted identifiers. These functions
    // are meant to reduce the amount of code in the user code.

    template <typename IDTYPE>
    Function UNIX_TIMESTAMP(IDTYPE const& column) const {
        return Function::UNIX_TIMESTAMP(sqlId(column));
    }

    template <typename IDTYPE1, typename IDTYPE2>
    Function TIMESTAMPDIFF(std::string const& resolution, IDTYPE1 const& lhs, IDTYPE2 const& rhs) const {
        return Function::TIMESTAMPDIFF(resolution, sqlId(lhs), sqlId(rhs));
    }

    // Generator: [cond1 [AND cond2 [...]]]

    /// The end of variadic recursion
    void sqlPackCondition(std::string& sql) const {}

    /// The next step in the variadic recursion when at least one condition is
    /// still available
    template <typename... Targs>
    void sqlPackCondition(std::string& sql, std::string const& cond, Targs... Fargs) const {
        if (!cond.empty()) sql += std::string(sql.empty() ? "" : " AND ") + cond;
        sqlPackCondition(sql, Fargs...);
    }

    template <typename... Targs>
    std::string sqlPackConditions(Targs... Fargs) const {
        std::string sql;
        sqlPackCondition(sql, Fargs...);
        return sql;
    }

    /**
     * Turn values of variadic arguments into a valid SQL representing a well-formed
     * 'WHERE' clause of a query to. Input parameters of the method must be of one
     * the following types: 'std::string const&', 'char const*', or any other type
     * accepted by the the operator:
     * @code
     * std::string::operator+=
     * @endcode
     * For example, the following call:
     * @code
     *   std::vector<std::string>> const databases = {"dbA", "dbB"};
     *   sqlWhere(
     *       sqlEq("col", 123),
     *       sqlIn("database", databases));
     * @endcode
     * This will produce the following output:
     * @code
     *   " WHERE `col`=123 AND `database` IN ('dbA', 'dbB')""
     * @endcode
     */
    template <typename... Targs>
    std::string sqlWhere(Targs... Fargs) const {
        std::string const sql = sqlPackConditions(Fargs...);
        return sql.empty() ? sql : " WHERE " + sql;
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
    template <typename... Targs>
    std::string sqlInsert(std::string const& tableName, Targs... Fargs) const {
        return "INSERT INTO " + sqlId(tableName).str + " VALUES (" + sqlPackValues(Fargs...) + ")";
    }

    std::string sqlInsertPacked(std::string const& tableName, std::string const& packedColumns,
                                std::string const& packedValues,
                                std::string const& values2update = std::string()) const {
        std::string sql = "INSERT INTO " + sqlId(tableName).str + " (" + packedColumns + ") VALUES (" +
                          packedValues + ")";
        if (!values2update.empty()) sql += " ON DUPLICATE KEY UPDATE " + values2update;
        return sql;
    }

    /**
     * @brief Generate and return an SQL expression for a binary operator applied
     *   over a pair of the pre-processed expressions.
     * @param lhs the preprocessed expression on the left side
     * @param rhs the preprocessed expression on the right side
     * @param op the binary operator
     * @return "<col><binary operator><value>"
     */
    template <typename LHS, typename RHS>
    std::string sqlBinaryOperator(LHS const& lhs, RHS const& rhs, std::string const& op) const {
        return lhs.str + op + rhs.str;
    }

    /**
     * @return "<quoted-col>=<escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), "=");
    }

    std::string sqlEqual(DoNotProcess const& lhs, DoNotProcess const& rhs) const {
        return sqlBinaryOperator(lhs, rhs, "=");
    }

    /**
     * @return "<quoted-col> != <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlNotEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), "!=");
    }

    /**
     * @return "<quoted-col> < <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLess(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), "<");
    }

    /**
     * @return "<quoted-col> <= <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLessOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), "<=");
    }

    /**
     * @return "<quoted-col> > <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreater(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), ">");
    }

    template <typename T>
    std::string sqlGreater(DoNotProcess const& lhs, T const& val) const {
        return sqlBinaryOperator(lhs, sqlValue(val), ">");
    }

    /**
     * @return "<quoted-col> => <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreaterOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(val), ">=");
    }

    /**
     * @return "<quoted-col> REGEXP <escaped-quoted-expr>"
     */
    std::string sqlRegexp(std::string const& col, std::string const& expr) const {
        return sqlBinaryOperator(sqlId(col), sqlValue(expr), " REGEXP ");
    }

    /// The base (the final function) to be called
    void sqlPackPair(std::string&) const {}

    /// Recursive variadic function (overloaded for column names given as std::string)
    template <typename T, typename... Targs>
    void sqlPackPair(std::string& sql, std::pair<std::string, T> colVal, Targs... Fargs) const {
        std::string const& col = colVal.first;
        T const& val = colVal.second;
        sql += (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) + sqlEqual(col, val);
        sqlPackPair(sql, Fargs...);
    }

    /// Recursive variadic function (overloaded for column names given as char const*)
    template <typename T, typename... Targs>
    void sqlPackPair(std::string& sql, std::pair<char const*, T> colVal, Targs... Fargs) const {
        std::string const col = colVal.first;
        T const& val = colVal.second;
        sql += (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) + sqlEqual(col, val);
        sqlPackPair(sql, Fargs...);
    }

    /**
     * Pack pairs of column names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     * @code
     *   UPDATE <table> SET <packed-pairs>
     * @endcode
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
     * @endcode
     * will produce the following string content:
     * @code
     *     "`col1`='st\'r',`col2`="c",`col3`=123,`fk_id`=LAST_INSERT_ID()""
     * @endcode
     *
     * @param Fargs the variadic list of values to be inserted
     */
    template <typename... Targs>
    std::string sqlPackPairs(Targs... Fargs) const {
        std::string sql;
        sqlPackPair(sql, Fargs...);
        return sql;
    }

    /**
     * @brief The generator for SQL clauses "IN" and "NOT IN".
     * @param col The name of a column (to be quoted as such).
     * @param values An iterable collection of values that will be escaped and
     *   quoted as needed.
     * @param complementary If "true" then generate "NOT IN". Otherwise "IN".
     * @return "`col` IN (<val1>,<val2>,<val3>,,,)", "`col` NOT IN (<val1>,<val2>,<val3>,,,)",
     *   or the empty string if the collection of values is empty.
     */
    template <typename T>
    std::string sqlIn(std::string const& col, T const& values, bool complementary = false) const {
        std::string sql;
        if (values.empty()) return sql;
        for (auto&& v : values) {
            if (!sql.empty()) sql += ",";
            sql += sqlValue(v).str;
        }
        return sqlId(col).str + " " + (complementary ? "NOT " : "") + "IN (" + sql + ")";
    }

    /**
     * @brief The convenience method to generate SQL clause "NOT IN".
     * @see method QueryGenerator::sqlIn()
     */
    template <typename T>
    std::string sqlNotIn(std::string const& col, T const& values) const {
        bool const complementary = true;
        return sqlIn(col, values, complementary);
    }

    /**
     * @brief The generator for SQL clauses "IN" and "NOT IN" with a sub-query.
     * @param colNameOrId The name or an identifier of a column.
     * @param subQueryText A sub-query to be used for selecting rows.
     * @param complementary If "true" then generate "NOT IN". Otherwise "IN".
     * @return "`col` IN (<sub-query-text>)", "`col` NOT IN (<sub-query-text>)",
     *   or the empty string if the sub-query is empty.
     */
    template <typename IDTYPE>
    std::string sqlInSubQuery(IDTYPE const& colNameOrId, std::string const& subQueryText,
                              bool complementary = false) const {
        return subQueryText.empty() ? std::string() : sqlInSubQuery(colNameOrId, sqlSubQuery(subQueryText));
    }

    template <typename IDTYPE>
    std::string sqlInSubQuery(IDTYPE const& colNameOrId, DoNotProcess const& subQuery,
                              bool complementary = false) const {
        return sqlId(colNameOrId).str + " " + (complementary ? "NOT " : "") + "IN " + subQuery.str;
    }

    /**
     * @brief The convenience method to generate SQL clause "NOT IN" with a sub-query.
     * @see method QueryGenerator::sqlIn()
     */
    template <typename IDTYPE, typename SUBQUERY_TYPE>
    std::string sqlNotInSubQuery(IDTYPE const& colNameOrId, SUBQUERY_TYPE const& subQuery) const {
        bool const complementary = true;
        return sqlInSubQuery(colNameOrId, subQuery, complementary);
    }

    /// The base (the final function) to be called
    void sqlPackSorter(std::string&) const {}

    /// Recursive variadic function
    template <typename... Targs>
    void sqlPackSorter(std::string& sql, std::pair<std::string, std::string> colOrd, Targs... Fargs) const {
        std::string const& col = colOrd.first;
        std::string const& ord = colOrd.second;
        sql += (sql.empty() ? "" : ",") + sqlId(col).str + (ord.empty() ? "" : " " + ord);
        sqlPackSorter(sql, Fargs...);
    }

    template <typename... Targs>
    void sqlPackSorter(std::string& sql, std::pair<SqlId, std::string> colOrd, Targs... Fargs) const {
        SqlId const& col = colOrd.first;
        std::string const& ord = colOrd.second;
        sql += (sql.empty() ? "" : ",") + col.str + (ord.empty() ? "" : " " + ord);
        sqlPackSorter(sql, Fargs...);
    }

    /**
     * @brief Generate the optional "ORDER BY" clause.
     * Pack pairs of column names and the optional sort ordering instructions
     * into a string which can be further used to form SQL clause of the following kind:
     * @code
     *   ORDER BY <packed-sorters>
     * @endcode
     * @note The method allows any number of arguments and the string (C++ or C)
     *   type values.
     *
     * For example, the following call:
     * @code
     *     sqlOrderBy(
     *         std::make_pair("col1", "ASC"),
     *         std::make_pair("col2", std::string("DESC")),
     *         std::make_pair("col3", ""));
     * @endcode
     * will produce the following string content:
     * @code
     *     " ORDER BY `col1` ASC,`col2` DESC,`col3`"
     * @endcode
     * @param Fargs the variadic list of the sorting instructions to be evaluated
     * @return The clause or the empty string if no sorting instructions were provided
     */
    template <typename... Targs>
    std::string sqlOrderBy(Targs... Fargs) const {
        std::string sql;
        sqlPackSorter(sql, Fargs...);
        return sql.empty() ? sql : " ORDER BY " + sql;
    }

    /// The base (the final function) to be called
    void sqlPackId(std::string&) const {}

    /// Recursive variadic function
    template <typename... Targs>
    void sqlPackId(std::string& sql, std::string const& col, Targs... Fargs) const {
        sql += (sql.empty() ? "" : ",") + sqlId(col).str;
        sqlPackId(sql, Fargs...);
    }

    /// Recursive variadic function (overloaded for SQL functions and keywords)
    template <typename... Targs>
    void sqlPackId(std::string& sql, DoNotProcess const& v, Targs... Fargs) const {
        sql += (sql.empty() ? "" : ",") + v.str;
        sqlPackId(sql, Fargs...);
    }

    /**
     * @brief Pack a collection of identifiers (columns, tables) or selectors in the SELECT
     * into the comma-separated sequence
     * For example, the following call:
     * @code
     *   sqlPackIds("col1", "col2", std::string("col3"));
     * @endcode
     * will produce the following string content:
     * @code
     *     "`col1`,`col2`,`col3`"
     * @endcode
     *
     * SQL functions and keywords could be also used here:
     * @code
     *   sqlPackIds("category", Function::COUNT_STAR);
     * @endcode
     * will produce the following string content:
     * @code
     *     "`category`,COUNT(*)"
     * @endcode
     * @param Fargs the variadic list of the identifiers/selectors to be evaluated
     * @return A string with the packed collection or the empty string if no columns
     *   were provided
     */
    template <typename... Targs>
    std::string sqlPackIds(Targs... Fargs) const {
        std::string sql;
        sqlPackId(sql, Fargs...);
        return sql;
    }

    /**
     * @brief Generate the optional "GROUP BY" clause.
     * Pack column names into a string which can be further used to form SQL clause
     * of the following kind:
     * @code
     *   GROUP BY <packed-columns>
     * @endcode
     * @note The method allows any number of arguments and the string (C++ or C)
     *   type values.
     *
     * For example, the following call:
     * @code
     *     sqlGroupBy("col1", "col2", std::string("col3"));
     * @endcode
     * will produce the following string content:
     * @code
     *     " GROUP BY `col1`,`col2`,`col3`"
     * @endcode
     * @param Fargs the variadic list of the columns to be evaluated
     * @return The clause or the empty string if no columns were provided
     */
    template <typename... Targs>
    std::string sqlGroupBy(Targs... Fargs) const {
        std::string const sql = sqlPackIds(Fargs...);
        return sql.empty() ? sql : " GROUP BY " + sql;
    }

    /**
     * @brief Generate the optional "LIMIT" clause.
     * For example, the following call:
     * @code
     *     sqlLimit(123)
     * @endcode
     * will produce the following string content:
     * @code
     *     " LIMIT 123"
     * @endcode
     * @param num The limit.
     * @return The complete clause or eh empty string of the limit was 0.
     */
    std::string sqlLimit(unsigned int num) const { return num == 0 ? "" : " LIMIT " + std::to_string(num); }

    /**
     * @brief Generate an SQL statement for updating select values of table rows.
     * Fields to be updated and their new values are passed into the method as
     * the variadic list of std::pair objects. The method allows any number of
     * arguments and any types of value types.
     * @note The method generates the partial query w/o the "WHERE" clause.
     * @code
     *   UPDATE <table> SET <packed-pairs>
     * @endcode
     *
     * For example:
     * @code
     *     sqlUpdate (
     *         "table",
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123))
     * @endcode
     * This will generate the following query:
     * @code
     *     "UPDATE `table` SET `col1`='st\'r',`col2`="c",`col3`=123"
     * @endcode
     *
     * @param tableName the name of a table
     * @param Fargs the variadic list of values to be inserted
     * @return well-formed SQL statement
     */
    template <typename... Targs>
    std::string sqlUpdate(std::string const& tableName, Targs... Fargs) const {
        return "UPDATE " + sqlId(tableName).str + " SET " + sqlPackPairs(Fargs...);
    }

    /**
     * @brief An SQL statement for deleting rows in the specified table.
     * @note The complete query should be made by concatenating the "WHERE"
     *   clause (using QueryGenerator::sqlWhere()) to the query if needed .
     * @param databaseName The optional name of a database.
     * @param tableName The name of a table.
     * @return A smart pointer to self to allow chained calls.
     */
    template <typename IDTYPE>
    std::string sqlDelete(IDTYPE const& tableNameOrId) const {
        return "DELETE FROM " + sqlId(tableNameOrId).str;
    }

    /**
     * @brief The generator for the table key specifications.
     * The method will produce the following:
     * @code
     *    <type> `<name>`[ (`<ref-col-1>`[,`<ref-col-2>`[...]])]
     * @endcode
     * For example, the following calls to the function:
     * @code
     *   sqlPackTableKey("PRIMARY KEY", "", "id")
     *   sqlPackTableKey("UNIQUE KEY", "", "col1", "col2")
     *   sqlPackTableKey("UNIQUE KEY", "composite", "col1", "col2", "col3")
     * @endcode
     * would result in the following outputs:
     * @code
     *   "PRIMARY KEY (`id`)"
     *   "UNIQUE KEY (`col1`,`col2`)"
     *   "UNIQUE KEY `composite` (`col1`,`col2`,`col3`)"
     * @endcode
     *
     * @param type The type of the key.
     * @param name The name of the key.
     * @param Fargs The names of columns referenced by the key (can be empty).
     * @return The generated key definition.
     */
    template <typename... Targs>
    std::string sqlPackTableKey(std::string const& type, std::string const& name, Targs... Fargs) const {
        std::string sql = type;
        if (!name.empty()) sql += " " + sqlId(name).str;
        std::string const sqlKeyRefs = sqlPackIds(Fargs...);
        if (!sqlKeyRefs.empty()) sql += " (" + sqlKeyRefs + ")";
        return sql;
    }

    /**
     * @brief Generate table creation query.
     * In this version of the generator, the table name will be used "as is" w/o
     * taking extra steps like turning the name into the properly quoted identifier.
     * The name is supposed to be prepared by a caller sing one of the following techniques:
     * @code
     *  std::string const tableName = sqlId("table");
     *  std::string const databaseTableName = sqlId("database", "table");
     * @endcode
     * @param id The prepared (quoted) table reference (that may include the name of a database).
     * @param ifNotExists If 'true' then generate "CREATE TABLE IF NOT" instead of just "CREATE TABLE".
     * @param columns Definition of the table columns (that include names and types of the columns).
     * @param keys The optional collection of the prepared table key specifications. The keys
     *   are supposed to be generated using the above defined method sqlPackTableKey.
     * @param engine The name of the table engine for generating "ENGINE=<engine>".
     * @param comment The optional comment. If a non-empty value will be provided then
     *   the resulting query will have "COMMENT='<comment>'";
     */
    std::string sqlCreateTable(SqlId const& id, bool ifNotExists, std::list<SqlColDef> const& columns,
                               std::list<std::string> const& keys = std::list<std::string>(),
                               std::string const& engine = "InnoDB",
                               std::string const& comment = std::string()) const;

    /**
     * @brief Generate table creation query (no database name is provided).
     */
    std::string sqlCreateTable(std::string const& tableName, bool ifNotExists,
                               std::list<SqlColDef> const& columns,
                               std::list<std::string> const& keys = std::list<std::string>(),
                               std::string const& engine = "InnoDB",
                               std::string const& comment = std::string()) const {
        return sqlCreateTable(sqlId(tableName), ifNotExists, columns, keys, engine, comment);
    }

    /**
     * @brief Generate table creation query (both database and table names are provided).
     */
    std::string sqlCreateTable(std::string const& databaseName, std::string const& tableName,
                               bool ifNotExists, std::list<SqlColDef> const& columns,
                               std::list<std::string> const& keys = std::list<std::string>(),
                               std::string const& engine = "InnoDB",
                               std::string const& comment = std::string()) const {
        return sqlCreateTable(sqlId(databaseName, tableName), ifNotExists, columns, keys, engine, comment);
    }

    template <typename IDTYPE1, typename IDTYPE2>
    std::string sqlCreateTableLike(IDTYPE1 const& newTableNameOrId, IDTYPE2 const& protoTableNameOrId,
                                   bool ifNotExists = false) const {
        std::string sql = "CREATE TABLE ";
        if (ifNotExists) sql += "IF NOT EXISTS ";
        sql += sqlId(newTableNameOrId).str + " LIKE " + sqlId(protoTableNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "DROP TABLE [IF EXISTS] `<table>`".
     * @param tableNameOrId The name or the preprocessed (sqlId(name)) identifier of a database.
     * @param ifExists If 'true' then add 'IF EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string sqlDropTable(IDTYPE const& tableNameOrId, bool ifExists = false) const {
        std::string sql = "DROP TABLE ";
        if (ifExists) sql += "IF EXISTS ";
        sql += sqlId(tableNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "CREATE DATABASE [IF NOT EXISTS] `<database>`".
     * @param databaseNameOrId The name or the preprocessed (sqlId(name)) identifier of a database.
     * @param ifNotExists If 'true' then add 'IF NOT EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string sqlCreateDatabase(IDTYPE const& databaseNameOrId, bool ifNotExists = false) const {
        std::string sql = "CREATE DATABASE ";
        if (ifNotExists) sql += "IF NOT EXISTS ";
        sql += sqlId(databaseNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "DROP DATABASE [IF EXISTS] `<database>`".
     * @param databaseNameOrId The name or the preprocessed (sqlId(name)) identifier of a database.
     * @param ifExists If 'true' then add 'IF EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string sqlDropDatabase(IDTYPE const& databaseNameOrId, bool ifExists = false) const {
        std::string sql = "DROP DATABASE ";
        if (ifExists) sql += "IF EXISTS ";
        sql += sqlId(databaseNameOrId).str;
        return sql;
    }

    /**
     * @brief The generator for REPLACE INTO
     * @code
     *   "REPLACE INTO `<database>`.`<table>` VALUES (...)"
     * @endcode
     * @param databaseName The optional name of a database.
     * @param tableName The name of a table.
     * @param Fargs Values to be replaced (the number and the order is required to match
     *   the corresponding columns as per the table schema).
     */
    template <typename... Targs>
    std::string sqlReplace(std::string const& databaseName, std::string const& tableName,
                           Targs... Fargs) const {
        return "REPLACE INTO " +
               (databaseName.empty() ? sqlId(tableName).str : sqlId(databaseName, tableName).str) +
               sqlValues(Fargs...);
    }

    /**
     * @brief Generator for "`<table>`.`<column>` AS `<id>`" for using in the SELECT
     *   queries (in the SELECT list and the FROM list).
     * @note Overloaded versions of the method are following this definition.
     * @param tableName The name of a table.
     * @param columnName The name of a column.
     * @param id The identifier to be injected.
     * @return The generated fragment of a query that doesn't require any further
     *   processing.
     */
    DoNotProcess sqlAs(std::string const& tableName, std::string const& columnName,
                       std::string const& id) const {
        return sqlAs(sqlId(tableName, columnName), id);
    }

    template <typename IDTYPE1, typename IDTYPE2>
    DoNotProcess sqlAs(IDTYPE1 const& lhs, IDTYPE2 const& rhs) const {
        return DoNotProcess(sqlId(lhs).str + " AS " + sqlId(rhs).str);
    }

    /**
     * @brief Generator for FROM ...
     * @param Fargs A collection of tables to select from.
     * @return The generated fragment of a query.
     */
    template <typename... Targs>
    std::string sqlFrom(Targs... Fargs) const {
        return " FROM " + sqlPackIds(Fargs...);
    }

    /**
     * @brief Generator for SELECT ...
     * @param Fargs A collection of tables to select from.
     * @return The generated fragment of a query.
     */
    template <typename... Targs>
    std::string sqlSelect(Targs... Fargs) const {
        return "SELECT " + sqlPackIds(Fargs...);
    }

    template <typename... Targs>
    std::string sqlRestrictByPartition(Targs... Fargs) const {
        std::string const ids = sqlPackIds(Fargs...);
        if (!ids.empty()) return " PARTITION (" + ids + ")";
        return std::string();
    }

    std::string sqlIntoOutfile(std::string const& fileName,
                               csv::Dialect const& dialect = csv::Dialect()) const {
        return " INTO OUTFILE " + sqlValue(fileName).str + " " + dialect.sqlOptions();
    }

    // Generated predicates to support searches using the FULL TEXT indexes

    template <typename IDTYPE>
    std::string sqlMatchAgainst(IDTYPE const& column, std::string const& searchPattern,
                                std::string const& mode) const {
        return "MATCH(" + sqlId(column).str + ") AGAINST(" + sqlValue(searchPattern).str + " IN " + mode +
               " MODE)";
    }

    // Generators for the MySQL partitioned tables.

    template <typename IDTYPE>
    std::string sqlPartitionByList(IDTYPE const& column) const {
        return " PARTITION BY LIST (" + sqlId(column).str + ")";
    }

    std::string sqlPartition(TransactionId transactionId) const {
        return " (PARTITION " + sqlId("p" + std::to_string(transactionId)).str + " VALUES IN (" +
               std::to_string(transactionId) + "))";
    }

    // Generators for ALTER TABLE ...

    template <typename IDTYPE>
    std::string sqlAlterTable(IDTYPE const& table, std::string const& spec = std::string()) const {
        std::string sql = "ALTER TABLE " + sqlId(table).str;
        if (!spec.empty()) sql += " " + spec;
        return sql;
    }

    std::string sqlRemovePartitioning() const { return " REMOVE PARTITIONING"; }

    std::string sqlAddPartition(TransactionId transactionId, bool ifNotExists = false) const {
        std::string sql = " ADD PARTITION";
        if (ifNotExists) sql += " IF NOT EXISTS";
        sql += sqlPartition(transactionId);
        return sql;
    }

    std::string sqlDropPartition(TransactionId transactionId) const {
        return " DROP PARTITION " + sqlId("p" + std::to_string(transactionId)).str;
    }

    // Generators for LOAD DATA INFILE

    template <typename IDTYPE>
    std::string sqlLoadDataInfile(std::string const& fileName, IDTYPE const& tableNameOrId,
                                  bool local = false, csv::Dialect const& dialect = csv::Dialect()) const {
        std::string sql = "LOAD DATA ";
        if (local) sql += "LOCAL ";
        sql += "INFILE " + sqlValue(fileName).str + " INTO TABLE " + sqlId(tableNameOrId).str + " " +
               dialect.sqlOptions();
        return sql;
    }

    // Generators for table indexes

    template <typename IDTYPE>
    std::string sqlCreateIndex(IDTYPE const& tableNameOrId, std::string const& indexName,
                               std::string const& spec,
                               std::list<std::tuple<std::string, unsigned int, bool>> const& keys,
                               std::string const& comment = std::string()) const {
        return _sqlCreateIndex(sqlId(tableNameOrId), indexName, spec, keys, comment);
    }

    template <typename IDTYPE>
    std::string sqlShowIndexes(IDTYPE const& tableNameOrId) const {
        return "SHOW INDEXES FROM " + sqlId(tableNameOrId).str;
    }

    template <typename IDTYPE>
    std::string sqlDropIndex(IDTYPE const& tableNameOrId, std::string const& indexName) const {
        return "DROP INDEX " + sqlId(indexName).str + " ON " + sqlId(tableNameOrId).str;
    }

    // Generators for GRANT

    /// @return GRANT ... ON `<database>`.* ...
    std::string sqlGrant(std::string const& privileges, std::string const& database, std::string const& user,
                         std::string const& host) const {
        return "GRANT " + privileges + " ON " + sqlId(database, Function::STAR).str + " TO " +
               sqlValue(user).str + "@" + sqlValue(host).str;
    }

    /// @return GRANT ... ON `<database>`.`<table>` ...
    std::string sqlGrant(std::string const& privileges, std::string const& database, std::string const& table,
                         std::string const& user, std::string const& host) const {
        return "GRANT " + privileges + " ON " + sqlId(database, table).str + " TO " + sqlValue(user).str +
               "@" + sqlValue(host).str;
    }

private:
    std::string _sqlCreateIndex(SqlId const& tableId, std::string const& indexName, std::string const& spec,
                                std::list<std::tuple<std::string, unsigned int, bool>> const& keys,
                                std::string const& comment) const;

    /// The optional connection is set by the class's constructor.
    std::shared_ptr<Connection> _conn;
};

}  // namespace lsst::qserv::replica::database::mysql

#endif  // LSST_QSERV_REPLICA_DATABASEMYSQLGENERATOR_H