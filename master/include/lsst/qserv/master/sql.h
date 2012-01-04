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
 
#ifndef LSST_QSERV_MASTER_SQL_H
#define LSST_QSERV_MASTER_SQL_H
// sql.h - SQL interface module.  Convenience code/abstraction layer
// fro calling into MySQL.  Uncertain of how this usage conflicts with
// db usage via the python MySQLdb api. 


// Standard
#include <string>

// Boost
#include <boost/thread.hpp>
// MySQL
#include <mysql/mysql.h> // MYSQL is typedef, so we can't forward declare it.

namespace lsst {
namespace qserv {
namespace master {

/// class SqlConfig : Value class for configuring the MySQL connection
class SqlConfig {
public:
    SqlConfig() : port(0) {}
    std::string hostname;
    std::string username;
    std::string password;
    std::string dbName;
    unsigned int port;
    std::string socket;
};

/// class SqlConnection : Class for interacting with a MySQL database.
class SqlConnection {
public:
    SqlConnection(SqlConfig const& sc, bool useThreadMgmt=false); 
    ~SqlConnection(); 
    bool connectToDb();
    bool apply(std::string const& sql);

    char const* getMySqlError() const { return _mysqlError; }
    int getMySqlErrno() const { return _mysqlErrno; }
private:
    bool _init();
    bool _connect();
    void _discardResults(MYSQL* mysql);
    void _storeMysqlError(MYSQL* c);

    MYSQL* _conn;
    std::string _error;
    int _mysqlErrno;
    const char* _mysqlError;
    SqlConfig _config;
    bool _connected;
    bool _useThreadMgmt;
    static boost::mutex _sharedMutex;
    static bool _isReady;
}; // class SqlConnection


}}} // namespace lsst::qserv::master
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_MASTER_SQL_H
