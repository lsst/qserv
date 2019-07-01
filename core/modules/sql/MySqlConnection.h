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

#ifndef LSST_QSERV_SQL_MYSQLCONNECTION_H
#define LSST_QSERV_SQL_MYSQLCONNECTION_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConnection;
}
namespace sql {
    class MySqlConnection;
    class SqlConnectionFactory;
    class SqlResults;
}}}


namespace lsst {
namespace qserv {
namespace sql {

class MySqlResultIter : public SqlResultIter {
public:
    MySqlResultIter() : _columnCount(0) {}
    MySqlResultIter(mysql::MySqlConfig const& sc, std::string const& query);
    virtual ~MySqlResultIter() {}

    SqlErrorObject& getErrorObject() override { return _errObj; }
    StringVector const& operator*() const override { return _current; }
    SqlResultIter& operator++() override; // pre-increment iterator advance.
    bool done() const override; // Would like to relax LSST standard 3-4 for iterator classes

private:
    bool _setup(mysql::MySqlConfig const& sqlConfig, std::string const& query);

    std::shared_ptr<mysql::MySqlConnection> _connection;
    StringVector _current;
    SqlErrorObject _errObj;
    int _columnCount;
};


/// class SqlConnection : Class for interacting with a MySQL database.
class MySqlConnection : public SqlConnection {
public:
    virtual ~MySqlConnection();

    void reset(mysql::MySqlConfig const& sc, bool useThreadMgmt=false) override;

    bool connectToDb(SqlErrorObject&) override;

    bool selectDb(std::string const& dbName, SqlErrorObject&) override;

    bool runQuery(char const* query, int qSize,
                  SqlResults& results, SqlErrorObject&) override;

    bool runQuery(char const* query, int qSize, SqlErrorObject&) override;

    bool runQuery(std::string const query, SqlResults&,
                  SqlErrorObject&) override;

    /// with runQueryIter SqlConnection is busy until SqlResultIter is closed
    std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query) override;

    bool runQuery(std::string const query, SqlErrorObject&) override;

    bool dbExists(std::string const& dbName, SqlErrorObject&) override;

    bool createDb(std::string const& dbName, SqlErrorObject&,
                  bool failIfExists=true) override;

    bool createDbAndSelect(std::string const& dbName,
                           SqlErrorObject&,
                           bool failIfExists=true) override;


    bool dropDb(std::string const& dbName, SqlErrorObject&,
                bool failIfDoesNotExist=true) override;

    bool tableExists(std::string const& tableName,
                     SqlErrorObject&,
                     std::string const& dbName="") override;

    bool dropTable(std::string const& tableName,
                   SqlErrorObject&,
                   bool failIfDoesNotExist=true,
                   std::string const& dbName="") override;

    bool listTables(std::vector<std::string>&,
                    SqlErrorObject&,
                    std::string const& prefixed="",
                    std::string const& dbName="") override;

    void listColumns(std::vector<std::string>&,
                     SqlErrorObject&,
                     std::string const& dbName,
                     std::string const& tableName) override;

    std::string getActiveDbName() const override;

    /**
     *  Returns the value generated for an AUTO_INCREMENT column
     *  by the previous INSERT or UPDATE statement.
     */
    unsigned long long getInsertId() const override;

    /**
     *  Escape string for use inside SQL statements.
     *  @return an escaped string, or an empty string if the connection can not be established
     *  @note the connection MUST be connected before using this method
     */
    std::string escapeString(std::string const& rawString) const override;

    /**
     * Escape string for use inside SQL statements.
     * @return true if the escaped string could be created.
     * @note this method will attempt to connect if the connection is not already estabilshed.
     */
    bool escapeString(std::string const& rawString, std::string& escapedString,
            SqlErrorObject& errObj) override;

private:
    friend class MySqlResultIter;
    friend class sql::SqlConnectionFactory;

    // Private constructors; use SqlConnectionFactory to create new instances.
    MySqlConnection();
    MySqlConnection(mysql::MySqlConfig const& sc, bool useThreadMgmt=false);

    bool _init(SqlErrorObject&);
    bool _connect(SqlErrorObject&);
    bool _setErrorObject(SqlErrorObject&,
                         std::string const& details=std::string(""));
    std::shared_ptr<mysql::MySqlConnection> _connection;
};


}}} // namespace lsst::qserv::sql


#endif // LSST_QSERV_SQL_MYSQLCONNECTION_H
