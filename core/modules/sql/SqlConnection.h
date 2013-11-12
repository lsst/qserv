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
#ifndef LSST_QSERV_SQLCONNECTION_H
#define LSST_QSERV_SQLCONNECTION_H

// Standard
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "mysql/SqlConfig.hh"
#include "sql/SqlErrorObject.h"

namespace lsst {
namespace qserv {
// forward
class MySqlConnection;
class SqlResults;

class SqlResultIter {
public:
    typedef std::vector<std::string> List;
    SqlResultIter() {}
    SqlResultIter(SqlConfig const& sc, std::string const& query);
    SqlErrorObject& getErrorObject() { return _errObj; }

    List const& operator*() const { return _current; }
    SqlResultIter& operator++(); // pre-increment iterator advance.
    bool done() const; // Would like to relax LSST standard 3-4 for iterator classes

private:
    bool _setup(SqlConfig const& sqlConfig, std::string const& query);

    boost::shared_ptr<MySqlConnection> _connection;
    List _current;
    SqlErrorObject _errObj;
    int _columnCount;
};

/// class SqlConnection : Class for interacting with a MySQL database.
class SqlConnection {
public:
    SqlConnection();
    SqlConnection(SqlConfig const& sc, bool useThreadMgmt=false);
    ~SqlConnection();
    void reset(SqlConfig const& sc, bool useThreadMgmt=false);
    bool connectToDb(SqlErrorObject&);
    bool selectDb(std::string const& dbName, SqlErrorObject&);
    bool runQuery(char const* query, int qSize,
                  SqlResults& results, SqlErrorObject&);
    bool runQuery(char const* query, int qSize, SqlErrorObject&);
    bool runQuery(std::string const query, SqlResults&, SqlErrorObject&);
    /// with runQueryIter SqlConnection is busy until SqlResultIter is closed
    boost::shared_ptr<SqlResultIter> getQueryIter(std::string const& query);
    bool runQuery(std::string const query, SqlErrorObject&);
    bool dbExists(std::string const& dbName, SqlErrorObject&);
    bool createDb(std::string const& dbName, SqlErrorObject&,
                  bool failIfExists=true);
    bool createDbAndSelect(std::string const& dbName,
                           SqlErrorObject&,
                           bool failIfExists=true);
    bool dropDb(std::string const& dbName, SqlErrorObject&,
                bool failIfDoesNotExist=true);
    bool tableExists(std::string const& tableName,
                     SqlErrorObject&,
                     std::string const& dbName="");
    bool dropTable(std::string const& tableName,
                   SqlErrorObject&,
                   bool failIfDoesNotExist=true,
                   std::string const& dbName="");
    bool listTables(std::vector<std::string>&,
                    SqlErrorObject&,
                    std::string const& prefixed="",
                    std::string const& dbName="");

    std::string getActiveDbName() const;

    // Static helpers
    static void populateErrorObject(MySqlConnection& m, SqlErrorObject& o);

private:
    friend class SqlResultIter;
    bool _init(SqlErrorObject&);
    bool _connect(SqlErrorObject&);
    bool _setErrorObject(SqlErrorObject&,
                         std::string const& details=std::string(""));

    boost::shared_ptr<MySqlConnection> _connection;
}; // class SqlConnection


}} // namespace lsst::qserv
// Local Variables:
// mode:c++
// comment-column:0
// End:

#endif // LSST_QSERV_SQLCONNECTION_H
