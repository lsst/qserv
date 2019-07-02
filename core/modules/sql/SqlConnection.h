// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
// SqlConnection.h - The SqlConnection class provides a convenience
// layer on top of an underlying mysqlclient. Historically, the
// SqlConnection class abstracted every interaction with the database
// and provided some convenience functions (e.g., show tables, show
// databases) that went beyond providing a C++ wrapper to mysql. Some
// of the more raw mysql code has been moved to MySqlConnection, but
// not all.
// It is uncertain of how this usage conflicts with db usage via the
// python MySQLdb api, but no problems have been detected so far.

#ifndef LSST_QSERV_SQL_SQLCONNECTION_H
#define LSST_QSERV_SQL_SQLCONNECTION_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlErrorObject.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace sql {
    class SqlResults;
}}}


namespace lsst {
namespace qserv {
namespace sql {


class SqlResultIter {
public:
    virtual ~SqlResultIter() = default;

    virtual SqlErrorObject& getErrorObject() = 0;

    virtual StringVector const& operator*() const = 0;

    virtual SqlResultIter& operator++() = 0; // pre-increment iterator advance.

    virtual bool done() const = 0; // Would like to relax LSST standard 3-4 for iterator classes

protected:
    SqlResultIter() = default;
};


/// class SqlConnection : Class for interacting with a MySQL database.
class SqlConnection {
public:
    virtual ~SqlConnection() {};

    virtual void reset(mysql::MySqlConfig const& sc) = 0;

    virtual bool connectToDb(SqlErrorObject&) = 0;

    virtual bool selectDb(std::string const& dbName, SqlErrorObject&) = 0;

    virtual bool runQuery(char const* query, int qSize, SqlResults& results, SqlErrorObject&) = 0;

    virtual bool runQuery(char const* query, int qSize, SqlErrorObject&) = 0;

    virtual bool runQuery(std::string const query, SqlResults&, SqlErrorObject&) = 0;

    /// with runQueryIter SqlConnection is busy until SqlResultIter is closed
    virtual std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query) = 0;

    virtual bool runQuery(std::string const query, SqlErrorObject&) = 0;

    virtual bool dbExists(std::string const& dbName, SqlErrorObject&) = 0;

    virtual bool createDb(std::string const& dbName, SqlErrorObject&,
                          bool failIfExists=true) = 0;

    virtual bool createDbAndSelect(std::string const& dbName,
                                   SqlErrorObject&,
                                   bool failIfExists=true) = 0;

    virtual bool dropDb(std::string const& dbName, SqlErrorObject&, bool failIfDoesNotExist=true) = 0;

    virtual bool tableExists(std::string const& tableName,
                             SqlErrorObject&,
                             std::string const& dbName="") = 0;

    virtual bool dropTable(std::string const& tableName,
                           SqlErrorObject&,
                           bool failIfDoesNotExist=true,
                           std::string const& dbName="") = 0;

    virtual bool listTables(std::vector<std::string>&,
                            SqlErrorObject&,
                            std::string const& prefixed="",
                            std::string const& dbName="") = 0;

    /**
     * @brief Get the names of the columns in the given db and table
     *
     * @throws NoSuchDb, NoSuchTable, if the database or table do not exist, or an SqlException for other
     *         failures.
     *
     * @param dbName The name of the database to look in.
     * @param tableName The name of the table to look in.
     * @return std::vector<std::string> The column names.
     */
    virtual std::vector<std::string> listColumns(std::string const& dbName,
                                                 std::string const& tableName) = 0;

    virtual std::string getActiveDbName() const = 0;

    /**
     *  Returns the value generated for an AUTO_INCREMENT column
     *  by the previous INSERT or UPDATE statement.
     */
    virtual unsigned long long getInsertId() const = 0;

    /**
     *  Escape string for use inside SQL statements.
     *  @return an escaped string, or an empty string if the connection can not be established
     *  @note the connection MUST be connected before using this method
     */
    virtual std::string escapeString(std::string const& rawString) const = 0;

    /**
     * Escape string for use inside SQL statements.
     * @return true if the escaped string could be created.
     * @note this method will attempt to connect if the connection is not already estabilshed.
     */
    virtual bool escapeString(std::string const& rawString, std::string& escapedString,
                              SqlErrorObject& errObj) = 0;

protected:
    SqlConnection() = default;
};

}}}


#endif // LSST_QSERV_SQL_SQLCONNECTION_H
