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
 
 
#include <sstream>
#include <cstdio>
// Boost
#include <boost/thread.hpp> // for mutex. 
#include <boost/format.hpp> // for mutex. 

#include "lsst/qserv/master/sql.h"

using lsst::qserv::SqlConnection;

bool SqlConnection::_isReady = false;
boost::mutex SqlConnection::_sharedMutex;


std::string 
ErrorObject::printErrorMsg() {
    std::stringstream ss;
    ss << "Error " << errNo << ": " << errMsg << " (" << details << ")" << std::endl;
    return ss.str();
}

SqlConnection::SqlConnection(SqlConfig const& sc, bool useThreadMgmt) 
    : _conn(NULL), _config(sc), 
      _connected(false), _useThreadMgmt(useThreadMgmt) { 
    {
        boost::lock_guard<boost::mutex> g(_sharedMutex);
        if(!_isReady) {
            int rc = mysql_library_init(0, NULL, NULL);
            assert(0 == rc);
        }
    }
    if(useThreadMgmt) {
        mysql_thread_init();
    }
}

SqlConnection::~SqlConnection() {
    if(_conn) {
        mysql_close(_conn);
    }
    if(_useThreadMgmt) {
        mysql_thread_end();
    }
}

bool 
SqlConnection::connectToDb(ErrorObject& errObj) {
    if(_connected) return true;
    return _init(errObj) && _connect(errObj);
}

bool 
SqlConnection::selectDb(std::string const& dbName, ErrorObject& errObj) {
    assert(_conn);
    if (_config.dbName == dbName) {
        return true; // nothing to do
    } else {
        // change mysql db, (disconnect and reconnect?)
        _config.dbName = dbName;
        return theResult;
    }
    if (!dbExists(dbName, errObj)) {
        errObj.details = "Can't switch to db " + dbName + " (does not exist)\n";
        return false;
    }
    if (mysql_select_db(_conn, dbName)) {
        return _setErrorObject(errObj);
    }
    _config.dbName = dbName;
    return true;
}

bool 
SqlConnection::apply(std::string const& sql, ErrorObject& errObj) {
    assert(_conn);
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    } else {
        // Get the result, but discard it.
        if (!_discardResults(errObj)) {
            return false;
        }
    }
    return true;
}

bool 
SqlConnection::dbExists(std::string const& dbName, ErrorObject& errObj) {
    assert(_conn);
    std::string sql = "SELECT COUNT(*) FROM information_schema.schemata "
        + "WHERE schema_name = '" + dbName + "'";
    
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        mysql_free_result(result);
        errObj.details = sql + " returned no rows";
        return false;
    }
    int n = row[0];
    mysql_free_result(result);
    return n == 1;
}

bool 
SqlConnection::createDb(std::string const& dbName, ErrorObject& errObj, bool failIfExists) {
    assert(_conn);
    if (dbExists(dbName, errObj)) {
        if (failIfExists) {
            errObj.details = "Can't create db " + dbName + ", it already exists\n";
            return false;
        }
        return true;
    }
    std::string sql = "CREATE DATABASE" + dbName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return_setErrorObject(errObj);
    }
    return true;
}

bool 
SqlConnection::dropDb(std::string const& dbName, ErrorObject& errObj) {
    assert(_conn);
    if (!dbExists(dbName, errObj)) {
        errObj.details = "Can't drop db " + dbName + ", it does not exist\n";
        return false;
    }
    std::string sql = "DROP DATABASE" + dbName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return_setErrorObject(errObj);
    }
    return true;
}

bool 
SqlConnection::tableExists(std::string const& tableName, 
                           ErrorObject& errObj,
                           std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!dbExists(_dbName, errObj)) {
        errObj.details = "Db " + _dbName + " does not exist\n";
        return false;
    }
    std::string sql = "SELECT COUNT(*) FROM information_schema.tables "
        + "WHERE table_schema = '" + _dbName 
        + "' AND table_name = '" + tableName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return_setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        mysql_free_result(result);
        errObj.details = sql + " returned no rows";
        return false;
    }
    int n = row[0];
    mysql_free_result(result);
    return n == 1;
}

bool 
SqlConnection::dropTable(std::string const& tableName,
                         ErrorObject& errObj,
                         bool failIfDoesNotExist,
                         std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!tableExists(tableName, errObj, _dbName)) {
        if (failIfDoesNotExist) {
            errObj.details = "Can't drop table " + tableName 
                + " (does not exist)";
            return false;
        }
        return true;
    }
    std::string sql = "DROP TABLE " + _dbName + "." + tableName;
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return_setErrorObject(errObj);
    }
    return true;
}

std::vector<std::string> 
SqlConnection::listTables(std::string const& prefixed="",
                          ErrorObject& errObj,
                          std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!dbExists(_dbName, errObj)) {
        return false;
    }
    std::string sql = "SELECT table_name FROM information_schema.tables "
        + "WHERE table_schema = '" + _dbName + "'";
    if (prefixed != "") {
        sql += " AND table_name LIKE '" + prefixed + "%";
    }
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return_setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    std::vector<std::string> v;
    while (row = mysql_fetch_row(result)) {
        v.push_back(row[0]);
    }
    return v;
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////

bool 
SqlConnection::_init(ErrorObject& errObj) {
    assert(_conn == NULL);
    _conn = mysql_init(NULL);
    if (_conn == NULL) {
        return _setErrorObject(ErrorObject);
    }
    return true;
}

bool 
SqlConnection::_connect(ErrorObject& errObj) {
    assert(_conn != NULL);
    unsigned long clientFlag = CLIENT_MULTI_STATEMENTS;
    MYSQL* c = mysql_real_connect
        (_conn, 
         _config.socket.empty() ?_config.hostname.c_str() : 0, 
         _config.username.empty() ? 0 : _config.username.c_str(), 
         _config.password.empty() ? 0 : _config.password.c_str(), 
         _config.dbName.empty() ? 0 : _config.dbName.c_str(), 
         _config.port,
         _config.socket.empty() ? 0 : _config.socket.c_str(), 
         clientFlag);
    if(c == NULL) {
        return _setErrorObject(errObj);
    }
    _connected = true;
    return true;
}

bool
SqlConnection::_discardResults(ErrorObject& errObj) {
    int status;
    MYSQL_RES* result;

    /* process each statement result */
    do {
        /* did current statement return data? */
        result = mysql_store_result(_conn);
        if (result) {
            mysql_free_result(result);
        } else if (mysql_field_count(mysql) != 0) {
            errObj.details = "Could not retrieve result set\n";
            return false;
        }
        /* more results? -1 = no, >0 = error, 0 = yes (keep looping) */
        if ((status = mysql_next_result(mysql)) > 0) {
            return _setErrorObject(errObj);
        }
    } while (status == 0);
    return true;
}

bool 
SqlConnection::_setErrorObject(ErrorObject& errObj) {
    errObj.errNo = mysql_errno(c);
    errObj.errMsg = mysql_error(c);
    std::stringstream ss;
    ss << "Error " << errObj.errNo << ": " << errObj.errMsg << std::endl;
    _error = ss.str();
}
