/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// X houses the implementation of 

#include "MysqlConnection.h" // FIXME: switch to lsst/... namespace
#include "SqlConfig.hh"

namespace qsrv=lsst::qserv;
using lsst::qserv::MysqlConnection;
using lsst::qserv::SqlConfig;

// Statics
bool MysqlConnection::_mysqlReady = false;
boost::mutex MysqlConnection::_mysqlShared;

namespace { // File-scope helpers
inline void killMysql(MYSQL* mysql, bool useThreadMgmt) {
    mysql_close(mysql);
    if(useThreadMgmt) {
        mysql_thread_end(); 
    }
}
} // anonymous namespace

MysqlConnection::MysqlConnection() 
    : _mysql(NULL), _mysql_res(NULL) {
    _initMysql();
}

MysqlConnection::MysqlConnection(SqlConfig const& sqlConfig,
                                 bool useThreadMgmt) 
    : _sqlConfig(new SqlConfig(sqlConfig)), _useThreadMgmt(useThreadMgmt),
      _mysql(NULL), _mysql_res(NULL) {
    _initMysql();
}
    
MysqlConnection::~MysqlConnection() {
    if(_mysql) {
        if(_mysql_res) {
            MYSQL_ROW row;
            while(row = mysql_fetch_row(_mysql_res)); // Drain results.
            _mysql_res = NULL;
        }
        killMysql(_mysql, _useThreadMgmt);
    }
}

bool
MysqlConnection::connect() {
    // Cleanup garbage
    if(_mysql) killMysql(_mysql, _useThreadMgmt);
    _isConnected = false;
    // Make myself a thread
    if(_useThreadMgmt) {
        mysql_thread_init();
    }
    _mysql = mysql_init(NULL);
    if(!_mysql) return false;
    unsigned long clientFlag = CLIENT_MULTI_STATEMENTS;
    MYSQL* c = mysql_real_connect(
        _mysql, 
        _sqlConfig->socket.empty() ?_sqlConfig->hostname.c_str() : 0, 
        _sqlConfig->username.empty() ? 0 : _sqlConfig->username.c_str(), 
        _sqlConfig->password.empty() ? 0 : _sqlConfig->password.c_str(), 
        _sqlConfig->dbName.empty() ? 0 : _sqlConfig->dbName.c_str(), 
        _sqlConfig->port,
        _sqlConfig->socket.empty() ? 0 : _sqlConfig->socket.c_str(), 
        clientFlag);
    _isConnected = (c != NULL);
    return _isConnected;
}
bool
MysqlConnection::queryUnbuffered(std::string const& query) {
    // run query, store into list.
    int rc;
    rc = mysql_real_query(_mysql, query.c_str(), query.length());
    if(rc) { return false; }
    _mysql_res = mysql_use_result(_mysql);
    if(!_mysql_res) { return false; }
    return true;
}

////////////////////////////////////////////////////////////////////////
// MysqlConnection
// private:
////////////////////////////////////////////////////////////////////////
bool
MysqlConnection::_initMysql() {
    _isConnected = false; // reset.
    _mysql = NULL;
    boost::lock_guard<boost::mutex> g(_mysqlShared);
    if(!_mysqlReady) {
        int rc = mysql_library_init(0, NULL, NULL);
        assert(0 == rc);
        _mysqlReady = true;
    }
    return true;
}
