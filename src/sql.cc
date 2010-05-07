
#include <sstream>

// Boost
#include <boost/thread.hpp> // for mutex. 
#include <boost/format.hpp> // for mutex. 

#include "lsst/qserv/master/sql.h"

using lsst::qserv::master::SqlConnection;

SqlConnection::~SqlConnection() {
    if(!_conn) {
	mysql_close(_conn);
    }
}
bool SqlConnection::connectToResultDb() {

    return _init() && _connect();
}

bool SqlConnection::apply(std::string const& sql) {
    assert(_conn);

    MYSQL_RES* result;
    if (mysql_query(_conn, sql.c_str())) {
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
	 _socket.empty() ?_hostname.c_str() : 0, 
	 _username.empty() ? 0 : _username.c_str(), 
	 _password.empty() ? 0 : _password.c_str(), 
	 _dbName.empty() ? 0 : _dbName.c_str(), 
	 _port,
	 _socket.empty() ? 0 : _socket.c_str(), 
	 clientFlag);
    if(c == NULL) {
	_storeMysqlError(c);
	return false;
    }
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
	    printf("Could not execute statement\n");
    } while (status == 0);
        
}

void SqlConnection::_storeMysqlError(MYSQL* c) {
    _mysqlErrno = mysql_errno(c);
    _mysqlError = mysql_error(c);
    std::stringstream ss;
    ss << "Error " << _mysqlErrno << ": " << _mysqlError << std::endl;
    _error = ss.str();
}
