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
#include "replica/util/Common.h"
#include "replica/util/Csv.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class Connection;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica::database::mysql {

/**
 * Class DoNotProcess is an abstraction for SQL strings for cases when ordinary
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
    std::string const str;
};

/// This is an abstraction for pre-processed SQL identifiers.
using SqlId = DoNotProcess;

/**
 * Class Sql is an abstraction for SQL functions and keywords which needs to be
 * processed differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Sql : public DoNotProcess {
public:
    /// @return the object representing the SQL keyword 'NULL'
    static Sql const NULL_;

    /// @return an object representing the corresponding SQL function
    static Sql const LAST_INSERT_ID;

    /// @return an object representing the row counter selector
    static Sql const COUNT_STAR;

    /// @return an object representing the row selector
    static Sql const STAR;

    /// @return an object representing the current database selector
    static Sql const DATABASE;

    /// @return an object representing the current time selector
    static Sql const NOW;

    /// @param sqlId Preprocessed identifier of a column to be selected. The identifier
    ///  is expected to be made using id([table,]column).
    /// @return an object representing the function "UNIX_TIMESTAMP(<column>)"
    static Sql UNIX_TIMESTAMP(SqlId const& sqlId);

    /// @note Identifiers of the columns are expected to be formed using calls
    ///   to id(columnName).
    /// @param resolution The resolution of the result: "HOUR", "MINUTE", "SECOND", etc.
    ///   See MySQL documentation for further details.
    /// @param lhs Preprocessed identifier of the left column to be selected.
    /// @param rhs Preprocessed identifier of the left column to be selected.
    static Sql TIMESTAMPDIFF(std::string const& resolution, SqlId const& lhs, SqlId const& rhs);

    /// @param sqlVal A value of the required parameter of the procedure. The value is required to
    ///   be preprocessed.
    /// @return an object representing the procedure "QSERV_MANAGER(<sqlVal>)"
    static Sql QSERV_MANAGER(DoNotProcess const& sqlVal);

    /// @param str_ the input string
    explicit Sql(std::string const& str_) : DoNotProcess(str_) {}

    Sql() = delete;
    Sql(Sql const&) = default;
    Sql& operator=(Sql const&) = default;

    ~Sql() override = default;
};

/// The enumerator type dwefining a scope for a variable(s)
enum class SqlVarScope : int { SESSION = 1, GLOBAL = 2 };

/**
 * @brief Class QueryGenerator provides the API that facilitates generating MySQL queries.
 *
 * The class design allows using it in one of the three contexts:
 *
 * 1. As a subclass of a class that would override the virtual method QueryGenerator::escape()
 * for redirecting the operation to the corresponding MySQL function, or implementing
 * a custom function.
 *
 * 2. As a stand-alone class constructed with an existing connector object. This technique
 * would simplify using the generator in the user code. For example:
 * @code
 *   std::shared_ptr<Connection> conn = ...;
 *   QueryGenerator const g(conn);
 *   std::string const query = g.insert("constants", Sql::LAST_INSERT_ID, "pi", 3.14159);
 * @endcode
 *
 * 3. As a stand-alone class constructed without any connector. Objects constructed in this
 * way are meant to be used for unit tests. For example:
 * @code
 *   QueryGenerator const g;
 *   std::string const query = g.insert("table", Sql::LAST_INSERT_ID, "pi", 3.14159);
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
     * @brief The optional string processing algorithm that's could be replaced
     *   in a subclass. No processing is done in the default implementation of
     *   the method.
     * @param str The input string to be processed.
     * @return The result string ready to be used in the MySQL queries.
     */
    virtual std::string escape(std::string const& str) const;

    /// @return a non-escaped and back-tick-quoted SQL identifier
    SqlId id(std::string const& str) const { return SqlId("`" + str + "`"); }

    /// @return a composite identifier for a database and a table, or a table and a column
    SqlId id(std::string const& first, std::string const& second) const {
        return SqlId(id(first).str + "." + id(second).str);
    }

    /// @return the input as is
    SqlId id(SqlId const& sqlId) const { return sqlId; }

    /// @return special selector for generating code like: `table`.*
    SqlId id(std::string const& first, Sql const& second) const {
        return SqlId(id(first).str + "." + second.str);
    }

    /// @return a back-ticked identifier of a MySQL partition for the given "super-transaction"
    SqlId partId(TransactionId transactionId) const { return id("p" + std::to_string(transactionId)); }

    template <typename TYPEID>
    DoNotProcess distinctId(TYPEID const& nameOrId) const {
        return DoNotProcess("DISTINCT " + id(nameOrId).str);
    }

    // Type-specific value generators

    template <typename T>
    DoNotProcess val(T const& v) const {
        return DoNotProcess(std::to_string(v));
    }
    DoNotProcess val(bool const& v) const { return val(v ? 1 : 0); }
    DoNotProcess val(std::string const& str) const { return DoNotProcess("'" + escape(str) + "'"); }
    DoNotProcess val(char const* str) const { return val(std::string(str)); }
    DoNotProcess val(DoNotProcess const& v) const { return v; }
    DoNotProcess val(Sql const& f) const { return f; }
    DoNotProcess val(std::vector<std::string> const& coll) const;

    /**
     * The function replaces the "conditional operator" of C++ in SQL statements
     * generators. Unlike the standard operator this function allows internal
     * type switching while producing a result of a specific type.
     *
     * @return an object which doesn't require any further processing
     */
    DoNotProcess nullIfEmpty(std::string const& str) const {
        if (str.empty()) return Sql::NULL_;
        return val(str);
    }

    // Generator: ([value [, value [, ... ]]])
    // Where values of the string types will be surrounded with single quotes

    /// The end of variadic recursion
    void packVal(std::string& sql) const {}

    /// The next step in the variadic recursion when at least one value is
    /// still available
    template <typename T, typename... Targs>
    void packVal(std::string& sql, T v, Targs... Fargs) const {
        if (!sql.empty()) sql += ",";
        sql += val(v).str;
        // Recursively keep drilling down the list of arguments with one argument less.
        packVal(sql, Fargs...);
    }

    /**
     * Turn values of variadic arguments into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     * @code
     *   packVals("st'r", std::string("c"), 123, 24.5)
     * @endcode
     * This will produce the following output:
     * @code
     *   "'st\'r','c',123,24.5"
     * @endcode
     */
    template <typename... Targs>
    std::string packVals(Targs... Fargs) const {
        std::string sql;
        packVal(sql, Fargs...);
        return sql;
    }

    /// @return A sub-query object that requires no further processing.
    DoNotProcess subQuery(std::string const& subQuery) const { return DoNotProcess("(" + subQuery + ")"); }

    // Helper functions for the corresponding functions of the class Sql
    // would translate strings into the properly quoted identifiers. These functions
    // are meant to reduce the amount of code in the user code.

    template <typename IDTYPE>
    Sql UNIX_TIMESTAMP(IDTYPE const& column) const {
        return Sql::UNIX_TIMESTAMP(id(column));
    }

    template <typename IDTYPE1, typename IDTYPE2>
    Sql TIMESTAMPDIFF(std::string const& resolution, IDTYPE1 const& lhs, IDTYPE2 const& rhs) const {
        return Sql::TIMESTAMPDIFF(resolution, id(lhs), id(rhs));
    }

    Sql QSERV_MANAGER(std::string const& v) const { return Sql::QSERV_MANAGER(val(v)); }

    // Generator: [cond1 [AND cond2 [...]]]

    /// The end of variadic recursion
    void packCond(std::string& sql) const {}

    /// The next step in the variadic recursion when at least one condition is
    /// still available
    template <typename... Targs>
    void packCond(std::string& sql, std::string const& cond, Targs... Fargs) const {
        if (!cond.empty()) sql += std::string(sql.empty() ? "" : " AND ") + cond;
        packCond(sql, Fargs...);
    }

    template <typename... Targs>
    std::string packConds(Targs... Fargs) const {
        std::string sql;
        packCond(sql, Fargs...);
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
     *   where(
     *       eq("col", 123),
     *       in("database", databases));
     * @endcode
     * This will produce the following output:
     * @code
     *   " WHERE `col`=123 AND `database` IN ('dbA', 'dbB')""
     * @endcode
     */
    template <typename... Targs>
    std::string where(Targs... Fargs) const {
        std::string const sql = packConds(Fargs...);
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
    std::string insert(std::string const& tableName, Targs... Fargs) const {
        return "INSERT INTO " + id(tableName).str + " VALUES (" + packVals(Fargs...) + ")";
    }

    /**
     * @brief The INSERT query generator for cases when collections if the inserted values
     *   and the affected columns are already packed into strings.
     *
     * Use the variadic template method \name packIds to pack columns.
     * Use the variadic template metgod \packVals to pack values.
     * Here is an example:
     * @code
     *   QueryGenerator const g(conn);
     *   std::string const query =
     *       g.insertPacked("table",
     *                      g.packIds("id", "timestamp", "name"),
     *                      g.packVals(Sql::NULL_, Sql::NOW, "John Smith"));
     * @endcode
     *
     * @param tableName The name of a table where the row will be insert.
     * @param packedColumns A collection of column names packed into a string.
     * @param packedValues A collection of the values packed into a string.
     * @param values2update The optional pairs for updating existing values
     *   should the row already existed in the table (the key duplication event
     *   will be intercepted and processed).
     * @return The generated query.
     */
    std::string insertPacked(std::string const& tableName, std::string const& packedColumns,
                             std::string const& packedValues,
                             std::string const& values2update = std::string()) const {
        std::string sql =
                "INSERT INTO " + id(tableName).str + " (" + packedColumns + ") VALUES (" + packedValues + ")";
        if (!values2update.empty()) sql += " ON DUPLICATE KEY UPDATE " + values2update;
        return sql;
    }

    /**
     * @brief The INSERT query generator optimized for inserting many rows in
     *   a single statement.
     *
     * Here is an example:
     * @code
     *   QueryGenerator const g(conn);
     *   std::string const query =
     *       g.insertPacked("table",
     *                      g.packIds("id", "timestamp", "name"),
     *                      {g.packVals(Sql::NULL_, Sql::NOW, "John Smith"),
     *                       g.packVals(Sql::NULL_, Sql::NOW, "Vera Rubin"),
     *                       g.packVals(Sql::NULL_, Sql::NOW, "Igor Gaponenko")});
     * @endcode
     *
     * The maximum size of the query string in MySQL is determined by the server
     * variable which is not checked by the current implementation:
     * @code
     *   SHOW VARIABLES LIKE 'max_allowed_packet';
     * @endcode
     * @note In the future implementation of the generator, this may change by adding
     *   another version of the method that would return a collection of queries each
     *   being under this limit instead of a single string.
     *
     * @param tableName The name of a table where the rows will be insert.
     * @param packedColumns A collection of column names packed into a string.
     * @param packedValues A collection of the packed rows.
     * @return The generated query.
     * @throws std::invalid_argument If the collection of rows is empty.
     */
    std::string insertPacked(std::string const& tableName, std::string const& packedColumns,
                             std::vector<std::string> const& packedValues) const;

    /**
     * @brief Generate and return an SQL expression for a binary operator applied
     *   over a pair of the pre-processed expressions.
     * @param lhs the preprocessed expression on the left side
     * @param rhs the preprocessed expression on the right side
     * @param op the binary operator
     * @return "<col><binary operator><value>"
     */
    template <typename LHS, typename RHS>
    std::string op2(LHS const& lhs, RHS const& rhs, std::string const& op) const {
        return lhs.str + op + rhs.str;
    }

    /// @return "<quoted-col>=<escaped-quoted-value>"
    template <typename T>
    std::string eq(std::string const& col, T const& v) const {
        return op2(id(col), val(v), "=");
    }

    std::string eq(DoNotProcess const& lhs, DoNotProcess const& rhs) const { return op2(lhs, rhs, "="); }

    /// @return "<quoted-col> != <escaped-quoted-value>"
    template <typename T>
    std::string neq(std::string const& col, T const& v) const {
        return op2(id(col), val(v), "!=");
    }

    /// @return "<quoted-col> < <escaped-quoted-value>"
    template <typename T>
    std::string lt(std::string const& col, T const& v) const {
        return op2(id(col), val(v), "<");
    }

    /// @return "<quoted-col> <= <escaped-quoted-value>"
    template <typename T>
    std::string leq(std::string const& col, T const& v) const {
        return op2(id(col), val(v), "<=");
    }

    /// @return "<quoted-col> > <escaped-quoted-value>"
    template <typename T>
    std::string gt(std::string const& col, T const& v) const {
        return op2(id(col), val(v), ">");
    }

    template <typename T>
    std::string gt(DoNotProcess const& lhs, T const& v) const {
        return op2(lhs, val(v), ">");
    }

    /// @return "<quoted-col> => <escaped-quoted-value>"
    template <typename T>
    std::string geq(std::string const& col, T const& v) const {
        return op2(id(col), val(v), ">=");
    }

    /// @return "<quoted-col> REGEXP <escaped-quoted-expr>"
    std::string regexp(std::string const& col, std::string const& expr) const {
        return op2(id(col), val(expr), " REGEXP ");
    }

    /// @return "<quoted-col> LIKE <escaped-quoted-expr>"
    std::string like(std::string const& col, std::string const& expr) const {
        return op2(id(col), val(expr), " LIKE ");
    }

    /**
     * Pack pairs of column/variable names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     * @code
     *   UPDATE <table> SET <packed-pairs>
     * @endcode
     * @note The method allows any number of arguments and any types of values.
     * @note  Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * @note The column/variable names will be surrounded with back-tick quotes.
     *
     * For example, the following call:
     * @code
     *     packPairs(
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123),
     *         std::make_pair("fk_id", Sql::LAST_INSERT_ID));
     * @endcode
     * will produce the following string content:
     * @code
     *     "`col1`='st\'r',`col2`="c",`col3`=123,`fk_id`=LAST_INSERT_ID()""
     * @endcode
     *
     * @param Fargs the variadic list of values to be inserted
     */
    template <typename... Targs>
    std::string packPairs(Targs... Fargs) const {
        std::string sql;
        _packPair(sql, Fargs...);
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
    std::string in(std::string const& col, T const& values, bool complementary = false) const {
        std::string sql;
        if (values.empty()) return sql;
        for (auto&& v : values) {
            if (!sql.empty()) sql += ",";
            sql += val(v).str;
        }
        return id(col).str + " " + (complementary ? "NOT " : "") + "IN (" + sql + ")";
    }

    /**
     * @brief The convenience method to generate SQL clause "NOT IN".
     * @see method QueryGenerator::in()
     */
    template <typename T>
    std::string notIn(std::string const& col, T const& values) const {
        bool const complementary = true;
        return in(col, values, complementary);
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
    std::string inSubQuery(IDTYPE const& colNameOrId, std::string const& subQueryText,
                           bool complementary = false) const {
        return subQueryText.empty() ? std::string() : inSubQuery(colNameOrId, subQuery(subQueryText));
    }

    template <typename IDTYPE>
    std::string inSubQuery(IDTYPE const& colNameOrId, DoNotProcess const& subQuery,
                           bool complementary = false) const {
        return id(colNameOrId).str + " " + (complementary ? "NOT " : "") + "IN " + subQuery.str;
    }

    /**
     * @brief The convenience method to generate SQL clause "NOT IN" with a sub-query.
     * @see method QueryGenerator::in()
     */
    template <typename IDTYPE, typename SUBQUERY_TYPE>
    std::string notInSubQuery(IDTYPE const& colNameOrId, SUBQUERY_TYPE const& subQuery) const {
        bool const complementary = true;
        return inSubQuery(colNameOrId, subQuery, complementary);
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
     *     orderBy(
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
    std::string orderBy(Targs... Fargs) const {
        std::string sql;
        _packSorter(sql, Fargs...);
        return sql.empty() ? sql : " ORDER BY " + sql;
    }

    /**
     * @brief Pack a collection of identifiers (columns, tables) or selectors in the SELECT
     * into the comma-separated sequence
     * For example, the following call:
     * @code
     *   packIds("col1", "col2", std::string("col3"));
     * @endcode
     * will produce the following string content:
     * @code
     *     "`col1`,`col2`,`col3`"
     * @endcode
     *
     * SQL functions and keywords could be also used here:
     * @code
     *   packIds("category", Sql::COUNT_STAR);
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
    std::string packIds(Targs... Fargs) const {
        std::string sql;
        _packId(sql, Fargs...);
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
     *     groupBy("col1", "col2", std::string("col3"));
     * @endcode
     * will produce the following string content:
     * @code
     *     " GROUP BY `col1`,`col2`,`col3`"
     * @endcode
     * @param Fargs the variadic list of the columns to be evaluated
     * @return The clause or the empty string if no columns were provided
     */
    template <typename... Targs>
    std::string groupBy(Targs... Fargs) const {
        std::string const sql = packIds(Fargs...);
        return sql.empty() ? sql : " GROUP BY " + sql;
    }

    /**
     * @brief Generate the optional "LIMIT" clause.
     * For example, the following call:
     * @code
     *     limit(123)
     *     limit(123, 0)
     *     limit(123, 1)
     * @endcode
     * will produce the following string content:
     * @code
     *     " LIMIT 123"
     *     " LIMIT 123"
     *     " LIMIT 123 OFFSET 1"
     * @endcode
     * @param num The limit.
     * @return The complete clause or the empty string if the limit was 0.
     */
    std::string limit(unsigned int num, unsigned int offset = 0) const;

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
    std::string update(std::string const& tableName, Targs... Fargs) const {
        return "UPDATE " + id(tableName).str + " SET " + packPairs(Fargs...);
    }

    /**
     * @brief An SQL statement for deleting rows in the specified table.
     * @note The complete query should be made by concatenating the "WHERE"
     *   clause (using QueryGenerator::where()) to the query if needed .
     * @param databaseName The optional name of a database.
     * @param tableName The name of a table.
     * @return well-formed SQL statement
     */
    template <typename IDTYPE>
    std::string delete_(IDTYPE const& tableNameOrId) const {
        return "DELETE FROM " + id(tableNameOrId).str;
    }

    /**
     * @brief The generator for the table key specifications.
     * The method will produce the following:
     * @code
     *    <type> `<name>`[ (`<ref-col-1>`[,`<ref-col-2>`[...]])]
     * @endcode
     * For example, the following calls to the function:
     * @code
     *   packTableKey("PRIMARY KEY", "", "id")
     *   packTableKey("UNIQUE KEY", "", "col1", "col2")
     *   packTableKey("UNIQUE KEY", "composite", "col1", "col2", "col3")
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
    std::string packTableKey(std::string const& type, std::string const& name, Targs... Fargs) const {
        std::string sql = type;
        if (!name.empty()) sql += " " + id(name).str;
        std::string const sqlKeyRefs = packIds(Fargs...);
        if (!sqlKeyRefs.empty()) sql += " (" + sqlKeyRefs + ")";
        return sql;
    }

    /**
     * @brief Generate table creation query.
     * In this version of the generator, the table name will be used "as is" w/o
     * taking extra steps like turning the name into the properly quoted identifier.
     * The name is supposed to be prepared by a caller sing one of the following techniques:
     * @code
     *  std::string const tableName = id("table");
     *  std::string const databaseTableName = id("database", "table");
     * @endcode
     * @param sqlId The prepared (quoted) table reference (that may include the name of a database).
     * @param ifNotExists If 'true' then generate "CREATE TABLE IF NOT" instead of just "CREATE TABLE".
     * @param columns Definition of the table columns (that include names and types of the columns).
     * @param keys The optional collection of the prepared table key specifications. The keys
     *   are supposed to be generated using the above defined method sqlPackTableKey.
     * @param engine The name of the table engine for generating "ENGINE=<engine>".
     * @param comment The optional comment. If a non-empty value will be provided then
     *   the resulting query will have "COMMENT='<comment>'";
     */
    std::string createTable(SqlId const& sqlId, bool ifNotExists, std::list<SqlColDef> const& columns,
                            std::list<std::string> const& keys = std::list<std::string>(),
                            std::string const& engine = "InnoDB",
                            std::string const& comment = std::string()) const;

    /**
     * @brief Generate table creation query (no database name is provided).
     */
    std::string createTable(std::string const& tableName, bool ifNotExists,
                            std::list<SqlColDef> const& columns,
                            std::list<std::string> const& keys = std::list<std::string>(),
                            std::string const& engine = "InnoDB",
                            std::string const& comment = std::string()) const {
        return createTable(id(tableName), ifNotExists, columns, keys, engine, comment);
    }

    /**
     * @brief Generate table creation query (both database and table names are provided).
     */
    std::string createTable(std::string const& databaseName, std::string const& tableName, bool ifNotExists,
                            std::list<SqlColDef> const& columns,
                            std::list<std::string> const& keys = std::list<std::string>(),
                            std::string const& engine = "InnoDB",
                            std::string const& comment = std::string()) const {
        return createTable(id(databaseName, tableName), ifNotExists, columns, keys, engine, comment);
    }

    template <typename IDTYPE1, typename IDTYPE2>
    std::string createTableLike(IDTYPE1 const& newTableNameOrId, IDTYPE2 const& protoTableNameOrId,
                                bool ifNotExists = false) const {
        std::string sql = "CREATE TABLE ";
        if (ifNotExists) sql += "IF NOT EXISTS ";
        sql += id(newTableNameOrId).str + " LIKE " + id(protoTableNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "DROP TABLE [IF EXISTS] `<table>`".
     * @param tableNameOrId The name or the preprocessed (id(name)) identifier of a database.
     * @param ifExists If 'true' then add 'IF EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string dropTable(IDTYPE const& tableNameOrId, bool ifExists = false) const {
        std::string sql = "DROP TABLE ";
        if (ifExists) sql += "IF EXISTS ";
        sql += id(tableNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "CREATE DATABASE [IF NOT EXISTS] `<database>`".
     * @param databaseNameOrId The name or the preprocessed (id(name)) identifier of a database.
     * @param ifNotExists If 'true' then add 'IF NOT EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string createDb(IDTYPE const& databaseNameOrId, bool ifNotExists = false) const {
        std::string sql = "CREATE DATABASE ";
        if (ifNotExists) sql += "IF NOT EXISTS ";
        sql += id(databaseNameOrId).str;
        return sql;
    }

    /**
     * @brief Generate "DROP DATABASE [IF EXISTS] `<database>`".
     * @param databaseNameOrId The name or the preprocessed (id(name)) identifier of a database.
     * @param ifExists If 'true' then add 'IF EXISTS'.
     * @return The complete query.
     */
    template <typename IDTYPE>
    std::string dropDb(IDTYPE const& databaseNameOrId, bool ifExists = false) const {
        std::string sql = "DROP DATABASE ";
        if (ifExists) sql += "IF EXISTS ";
        sql += id(databaseNameOrId).str;
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
    std::string replace(std::string const& databaseName, std::string const& tableName, Targs... Fargs) const {
        return "REPLACE INTO " +
               (databaseName.empty() ? id(tableName).str : id(databaseName, tableName).str) +
               _values(Fargs...);
    }

    /**
     * @brief Generator for "`<table>`.`<column>` AS `<id>`" for using in the SELECT
     *   queries (in the SELECT list and the FROM list).
     * @note Overloaded versions of the method are following this definition.
     * @param tableName The name of a table.
     * @param columnName The name of a column.
     * @param aliasName The identifier to be injected.
     * @return The generated fragment of a query that doesn't require any further
     *   processing.
     */
    DoNotProcess as(std::string const& tableName, std::string const& columnName,
                    std::string const& aliasName) const {
        return as(id(tableName, columnName), aliasName);
    }

    template <typename IDTYPE1, typename IDTYPE2>
    DoNotProcess as(IDTYPE1 const& lhs, IDTYPE2 const& rhs) const {
        return DoNotProcess(id(lhs).str + " AS " + id(rhs).str);
    }

    /**
     * @brief Generator for FROM ...
     * @param Fargs A collection of tables to select from.
     * @return The generated fragment of a query.
     */
    template <typename... Targs>
    std::string from(Targs... Fargs) const {
        return " FROM " + packIds(Fargs...);
    }

    /**
     * @brief Generator for SELECT ...
     * @param Fargs A collection of tables to select from.
     * @return The generated fragment of a query.
     */
    template <typename... Targs>
    std::string select(Targs... Fargs) const {
        return "SELECT " + packIds(Fargs...);
    }

    template <typename... Targs>
    std::string inPartition(Targs... Fargs) const {
        std::string const ids = packIds(Fargs...);
        if (!ids.empty()) return " PARTITION (" + ids + ")";
        return std::string();
    }

    std::string intoOutfile(std::string const& fileName, csv::Dialect const& dialect = csv::Dialect()) const {
        return " INTO OUTFILE " + val(fileName).str + " " + dialect.sqlOptions();
    }

    // Generated predicates to support searches using the FULL TEXT indexes

    template <typename IDTYPE>
    std::string matchAgainst(IDTYPE const& column, std::string const& searchPattern,
                             std::string const& mode) const {
        return "MATCH(" + id(column).str + ") AGAINST(" + val(searchPattern).str + " IN " + mode + " MODE)";
    }

    // Generators for the MySQL partitioned tables.

    template <typename IDTYPE>
    std::string partitionByList(IDTYPE const& column) const {
        return " PARTITION BY LIST (" + id(column).str + ")";
    }

    std::string partition(TransactionId transactionId) const {
        return " (PARTITION " + partId(transactionId).str + " VALUES IN (" + std::to_string(transactionId) +
               "))";
    }

    // Generators for ALTER TABLE ...

    template <typename IDTYPE>
    std::string alterTable(IDTYPE const& table, std::string const& spec = std::string()) const {
        std::string sql = "ALTER TABLE " + id(table).str;
        if (!spec.empty()) sql += " " + spec;
        return sql;
    }

    std::string removePartitioning() const { return " REMOVE PARTITIONING"; }

    std::string addPartition(TransactionId transactionId, bool ifNotExists = false) const {
        std::string sql = " ADD PARTITION";
        if (ifNotExists) sql += " IF NOT EXISTS";
        sql += partition(transactionId);
        return sql;
    }

    /**
     * @brief Generate " DROP PARTITION [IF EXISTS] `p<transaction-id>`".
     * @param transactionId An identifier of the super-transaction corresponding to
     *   a partition to be removed.
     * @param ifExists If 'true' then add 'IF EXISTS'.
     * @return The complete query fragment.
     */
    std::string dropPartition(TransactionId transactionId, bool ifExists = false) const {
        std::string sql = " DROP PARTITION ";
        if (ifExists) sql += "IF EXISTS ";
        sql += partId(transactionId).str;
        return sql;
    }

    // Generators for LOAD DATA INFILE

    template <typename IDTYPE>
    std::string loadDataInfile(std::string const& fileName, IDTYPE const& tableNameOrId,
                               std::string const& charsetName = std::string(), bool local = false,
                               csv::Dialect const& dialect = csv::Dialect()) const {
        std::string sql = "LOAD DATA ";
        if (local) sql += "LOCAL ";
        sql += "INFILE " + val(fileName).str + " INTO TABLE " + id(tableNameOrId).str + " ";
        if (!charsetName.empty()) sql += "CHARACTER SET " + val(charsetName).str + " ";
        sql += dialect.sqlOptions();
        return sql;
    }

    // Generators for table indexes

    template <typename IDTYPE>
    std::string createIndex(IDTYPE const& tableNameOrId, std::string const& indexName,
                            std::string const& spec,
                            std::list<std::tuple<std::string, unsigned int, bool>> const& keys,
                            std::string const& comment = std::string()) const {
        return _createIndex(id(tableNameOrId), indexName, spec, keys, comment);
    }

    template <typename IDTYPE>
    std::string showIndexes(IDTYPE const& tableNameOrId) const {
        return "SHOW INDEXES FROM " + id(tableNameOrId).str;
    }

    template <typename IDTYPE>
    std::string dropIndex(IDTYPE const& tableNameOrId, std::string const& indexName) const {
        return "DROP INDEX " + id(indexName).str + " ON " + id(tableNameOrId).str;
    }

    // Generators for GRANT

    /// @return GRANT ... ON `<database>`.* ...
    std::string grant(std::string const& privileges, std::string const& database, std::string const& user,
                      std::string const& host) const {
        return "GRANT " + privileges + " ON " + id(database, Sql::STAR).str + " TO " + val(user).str + "@" +
               val(host).str;
    }

    /// @return GRANT ... ON `<database>`.`<table>` ...
    std::string grant(std::string const& privileges, std::string const& database, std::string const& table,
                      std::string const& user, std::string const& host) const {
        return "GRANT " + privileges + " ON " + id(database, table).str + " TO " + val(user).str + "@" +
               val(host).str;
    }

    /// @return SHOW WARNINGS
    std::string warnings() const { return "SHOW WARNINGS"; }

    /**
     * @brief Generator for an SQL squery that would return values of variables.
     *
     * For the following sample inputs:
     * @code
     *   showVars(SqlVarScope::GLOBAL);
     *   showVars(SqlVarScope::SESSION, "myisam_%");
     * @endcode
     * The generator will produce these statements:
     * @code
     *  SHOW GLOBAL VARIABLES
     *  SHOW VARIABLES LIKE 'myisam_%'
     * @code
     * @note The method will not validate the syntax of the pattern.
     * @param scope The scope of the variable (SESSION, GLOBAL, etc.)
     * @param pattern The optional pattern for searching the variables by names.
     *   If the empty string is passed as a value of the parameter then all variables
     *   in the specific scope will be reported by MySQL after executing the query.
     * @return Well-formed SQL statement.
     * @throws std::invalid_argument If the specified scope is not supported.
     */
    std::string showVars(SqlVarScope scope, std::string const& pattern = std::string()) const;

    /**
     * @brief Generator for setting variables in the given scope.
     *
     * For the following sample inputs:
     * @code
     *   setVars(SqlVarScope::GLOBAL,  std::make_mair("var1", 1);
     *   setVars(SqlVarScope::SESSION, std::make_mair("var2", 2), std::make_mair("var3", "abc"));
     * @endcode
     * The generator will produce these statements:
     * @code
     *  SET GLOBAL `var1`=1
     *  SET `var2`=2,`var3`='abc'
     * @code
     *
     * @param scope The scope of the variable (SESSION, GLOBAL, etc.)
     * @param Fargs A collection of pairs specifying variable names and their values.
     * @return Well-formed SQL statement.
     * @throws std::invalid_argument If the input collection is empty, or in case
     *   if the specified scope is not supported.
     */
    template <typename... Targs>
    std::string setVars(SqlVarScope scope, Targs... Fargs) const {
        return _setVars(scope, packPairs(Fargs...));
    }

    /**
     * @brief Generator for calling stored procedures.
     *
     * For the following sample input:
     * @code
     *   call(QSERV_MANAGER("abc"));
     * @endcode
     * The generator will produce this statement:
     * @code
     *  CALL QSERV_MANAGER('abc')
     * @code
     *
     * @param packedProcAndArgs The well-formed SQL for the procedure and its parameters
     * @return Well-formed SQL statement.
     * @throws std::invalid_argument If the input parameter is empty.
     */
    std::string call(DoNotProcess const& packedProcAndArgs) const;

private:
    /// @return A string that's ready to be included into the queries.
    template <typename... Targs>
    std::string _values(Targs... Fargs) const {
        return " VALUES (" + packVals(Fargs...) + ")";
    }

    /// The base (the final function) to be called
    void _packPair(std::string&) const {}

    /// Recursive variadic function (overloaded for column names given as std::string)
    template <typename T, typename... Targs>
    void _packPair(std::string& sql, std::pair<std::string, T> colVal, Targs... Fargs) const {
        std::string const& col = colVal.first;
        T const& v = colVal.second;
        sql += (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) + eq(col, v);
        _packPair(sql, Fargs...);
    }

    /// Recursive variadic function (overloaded for column names given as char const*)
    template <typename T, typename... Targs>
    void _packPair(std::string& sql, std::pair<char const*, T> colVal, Targs... Fargs) const {
        std::string const col = colVal.first;
        T const& v = colVal.second;
        sql += (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) + eq(col, v);
        _packPair(sql, Fargs...);
    }

    /// The base (the final function) to be called
    void _packSorter(std::string&) const {}

    /// Recursive variadic function
    template <typename... Targs>
    void _packSorter(std::string& sql, std::pair<std::string, std::string> colOrd, Targs... Fargs) const {
        std::string const& col = colOrd.first;
        std::string const& ord = colOrd.second;
        sql += (sql.empty() ? "" : ",") + id(col).str + (ord.empty() ? "" : " " + ord);
        _packSorter(sql, Fargs...);
    }

    template <typename... Targs>
    void _packSorter(std::string& sql, std::pair<SqlId, std::string> colOrd, Targs... Fargs) const {
        SqlId const& col = colOrd.first;
        std::string const& ord = colOrd.second;
        sql += (sql.empty() ? "" : ",") + col.str + (ord.empty() ? "" : " " + ord);
        _packSorter(sql, Fargs...);
    }

    /// The base (the final function) to be called
    void _packId(std::string&) const {}

    /// Recursive variadic function
    template <typename... Targs>
    void _packId(std::string& sql, std::string const& col, Targs... Fargs) const {
        sql += (sql.empty() ? "" : ",") + id(col).str;
        _packId(sql, Fargs...);
    }

    /// Recursive variadic function (overloaded for SQL functions and keywords)
    template <typename... Targs>
    void _packId(std::string& sql, DoNotProcess const& v, Targs... Fargs) const {
        sql += (sql.empty() ? "" : ",") + v.str;
        _packId(sql, Fargs...);
    }

    /// @brief The implementatin of the generator for setting variables.
    /// @param scope The scope of the variable (SESSION, GLOBAL, etc.)
    /// @param packedVars Partial SQL for setting values of the variables.
    /// @return The well-formed SQL for setting the variables
    /// @throws std::invalid_argument If a value of \param packedVars is empty,
    ///   or in case if the specified value of \param scope is not supported.
    std::string _setVars(SqlVarScope scope, std::string const& packedVars) const;

    std::string _createIndex(SqlId const& tableId, std::string const& indexName, std::string const& spec,
                             std::list<std::tuple<std::string, unsigned int, bool>> const& keys,
                             std::string const& comment) const;

    /// The optional connection is set by the class's constructor.
    std::shared_ptr<Connection> _conn;
};

}  // namespace lsst::qserv::replica::database::mysql

#endif  // LSST_QSERV_REPLICA_DATABASEMYSQLGENERATOR_H
