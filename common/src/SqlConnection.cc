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

#include "SqlConnection.hh"

using lsst::qserv::SqlConnection;

bool SqlConnection::_isReady = false;
boost::mutex SqlConnection::_sharedMutex;


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
SqlConnection::connectToDb(SqlErrorObject& errObj) {
    if(_connected) return true;
    return _init(errObj) && _connect(errObj);
}

bool 
SqlConnection::selectDb(std::string const& dbName, SqlErrorObject& errObj) {
    assert(_conn);
    if (_config.dbName == dbName) {
        return true; // nothing to do
    } else {
        // change mysql db, (disconnect and reconnect?)
        _config.dbName = dbName;
        return true;
    }
    if (!dbExists(dbName, errObj)) {
        return errObj.addErrMsg (
            std::string("Can't switch to db ") + dbName + " (does not exist)");
    }
    if (mysql_select_db(_conn, dbName.c_str())) {
        return _setErrorObject(errObj);
    }
    _config.dbName = dbName;
    return true;
}

bool 
SqlConnection::apply(std::string const& sql, SqlErrorObject& errObj) {
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
SqlConnection::runQuery(char const* query, 
                        int qSize,
                        SqlErrorObject& errObj,
                        std::string arg) {
    if (arg.size() != 0) {
        // TODO -- bind arg
    }
    if (mysql_real_query(_conn, query, qSize) != 0) {
        MYSQL_RES* result = mysql_store_result(_conn);
        if (result) mysql_free_result(result);
        std::string msg = std::string("Unable to execute query: ") + query;
        //    + "\nQuery = " + std::string(query, qSize);
        return _setErrorObject(errObj, msg);
    }
    int status = 0;
    do {
        MYSQL_RES* result = mysql_store_result(_conn);
        if (result) {
            // TODO -- Do something with it?
            mysql_free_result(result);
        }
        else if (mysql_field_count(_conn) != 0) {
            return _setErrorObject(errObj, 
                      std::string("Unable to store result for query: ")
                                  + std::string(query,qSize));
        }
        status = mysql_next_result(_conn);
        if (status > 0) {
            return _setErrorObject(errObj, 
                  std::string("Error retrieving results for query: ") + query);
        }
    } while (status == 0);
    return true;
}

bool 
SqlConnection::runQuery(std::string const query, 
                        std::string arg, 
                        SqlErrorObject& errObj) {
    return runQuery(query.data(), query.size(), errObj, arg);
}

bool 
SqlConnection::dbExists(std::string const& dbName, SqlErrorObject& errObj) {
    assert(_conn);
    std::string sql = "SELECT COUNT(*) FROM information_schema.schemata ";
    sql += "WHERE schema_name = '";
    sql += dbName + "'";
    
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        mysql_free_result(result);
        return errObj.addErrMsg(sql + " returned no rows");
    }
    char n = *(row[0]);
    mysql_free_result(result);
    return n == '1';
}

bool 
SqlConnection::createDb(std::string const& dbName, 
                        SqlErrorObject& errObj, 
                        bool failIfExists) {
    assert(_conn);
    if (dbExists(dbName, errObj)) {
        if (failIfExists) {
            return errObj.addErrMsg(std::string("Can't create db ") 
                                    + dbName + ", it already exists");
        }
        return true;
    }
    std::string sql = "CREATE DATABASE" + dbName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    return true;
}

bool 
SqlConnection::dropDb(std::string const& dbName, 
                      SqlErrorObject& errObj,
                      bool failIfExists) {
    assert(_conn);
    if (!dbExists(dbName, errObj)) {
        if ( failIfExists ) {
            return errObj.addErrMsg(std::string("Can't drop db ")
                                    + dbName + ", it does not exist");
        }
        return true;
    }
    std::string sql = "DROP DATABASE" + dbName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    return true;
}

bool 
SqlConnection::tableExists(std::string const& tableName, 
                           SqlErrorObject& errObj,
                           std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!dbExists(_dbName, errObj)) {
        return errObj.addErrMsg(std::string("Db ")+_dbName+" does not exist");
    }
    std::string sql = "SELECT COUNT(*) FROM information_schema.tables ";
    sql += "WHERE table_schema = '";
    sql += _dbName + "' AND table_name = '" + tableName + "'";
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        mysql_free_result(result);
        return errObj.addErrMsg(sql + " returned no rows");
    }
    char n = *(row[0]);
    mysql_free_result(result);
    return n == '1';
}

bool 
SqlConnection::dropTable(std::string const& tableName,
                         SqlErrorObject& errObj,
                         bool failIfDoesNotExist,
                         std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!tableExists(tableName, errObj, _dbName)) {
        if (failIfDoesNotExist) {
            return errObj.addErrMsg(std::string("Can't drop table ")
                                    + tableName + " (does not exist)");
        }
        return true;
    }
    std::string sql = "DROP TABLE " + _dbName + "." + tableName;
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    return true;
}

bool
SqlConnection::listTables(std::vector<std::string>& v, 
                          SqlErrorObject& errObj,
                          std::string const& prefixed,
                          std::string const& dbName) {
    assert(_conn);
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!dbExists(_dbName, errObj)) {
        return false;
    }
    std::string sql = "SELECT table_name FROM information_schema.tables ";
    sql += "WHERE table_schema = '" + _dbName + "'";
    if (prefixed != "") {
        sql += " AND table_name LIKE '" + prefixed + "%";
    }
    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
        return _setErrorObject(errObj);
    }
    MYSQL_RES *result = mysql_store_result(_conn);
    MYSQL_ROW row;
    while (row = mysql_fetch_row(result)) {
        v.push_back(row[0]);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////

bool 
SqlConnection::_init(SqlErrorObject& errObj) {
    assert(_conn == NULL);
    _conn = mysql_init(NULL);
    if (_conn == NULL) {
        return _setErrorObject(errObj);
    }
    return true;
}

bool 
SqlConnection::_connect(SqlErrorObject& errObj) {
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
SqlConnection::_discardResults(SqlErrorObject& errObj) {
    int status;
    MYSQL_RES* result;

    /* process each statement result */
    do {
        /* did current statement return data? */
        result = mysql_store_result(_conn);
        if (result) {
            mysql_free_result(result);
        } else if (mysql_field_count(_conn) != 0) {
            return errObj.addErrMsg("Could not retrieve result set");
        }
        /* more results? -1 = no, >0 = error, 0 = yes (keep looping) */
        if ((status = mysql_next_result(_conn)) > 0) {
            return _setErrorObject(errObj);
        }
    } while (status == 0);
    return true;
}

bool 
SqlConnection::_setErrorObject(SqlErrorObject& errObj, 
                               std::string const& extraMsg) {
    errObj.setErrNo( mysql_errno(_conn) );
    errObj.addErrMsg( mysql_error(_conn) );
    if ( ! extraMsg.empty() ) {
        errObj.addErrMsg(extraMsg);
    }
}
