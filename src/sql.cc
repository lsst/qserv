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

using lsst::qserv::master::SqlConnection;

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
bool SqlConnection::connectToDb() {
    if(_connected) return true;
    return _init() && _connect();
}

bool SqlConnection::apply(std::string const& sql) {
    assert(_conn);

    if (mysql_real_query(_conn, sql.c_str(), sql.size())) {
	_storeMysqlError(_conn);
	return false;
    } else {
	// Get the result, but discard it.
	_discardResults(_conn);
    }
    return _error.empty();
}
////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////

bool SqlConnection::_init() {
    assert(_conn == NULL);
    _conn = mysql_init(NULL);
    if (_conn == NULL) {
	_storeMysqlError(_conn);
	return false;
    }
    return true;
}

bool SqlConnection::_connect() {
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
	_storeMysqlError(c);
	return false;
    }
    _connected = true;
    return true;
}

void SqlConnection::_discardResults(MYSQL* mysql) {
    int status;
    MYSQL_RES* result;

    /* process each statement result */
    do {
	/* did current statement return data? */
	result = mysql_store_result(mysql);
	if (result) {
	    mysql_free_result(result);
	} else if (mysql_field_count(mysql) != 0) {
	    _error = "Could not retrieve result set\n";
	    break;
	}
	/* more results? -1 = no, >0 = error, 0 = yes (keep looping) */
	if ((status = mysql_next_result(mysql)) > 0)
            std::printf("Could not execute statement\n");
    } while (status == 0);
        
}

void SqlConnection::_storeMysqlError(MYSQL* c) {
    _mysqlErrno = mysql_errno(c);
    _mysqlError = mysql_error(c);
    std::stringstream ss;
    ss << "Error " << _mysqlErrno << ": " << _mysqlError << std::endl;
    _error = ss.str();
}
