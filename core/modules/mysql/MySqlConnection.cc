// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
// MySqlConnection.cc houses the definition of methods in the
// MySqlConnection class. Eventually most (if not all) mysql_*
// function invocations should move into this class (and perhaps its
// delegates).

// Class header
#include "mysql/MySqlConnection.h"

// Third-party headers
#include "boost/make_shared.hpp"
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "mysql/MySqlConfig.h"


namespace { // File-scope helpers
inline void killMySql(MYSQL* mysql, bool useThreadMgmt) {
    mysql_close(mysql);
    // Dangerous to use mysql_thread_end(), because caller may belong to a
    // different thread other than the one that called mysql_init(). Suggest
    // using thread-local-storage to track users of mysql_init(), and to call
    // mysql_thread_end() appropriately. Not an easy thing to do right now, and
    // shouldn't be a big deal because we thread-pool anyway.
}
} // anonymous namespace


namespace lsst {
namespace qserv {
namespace mysql {

// Statics
bool MySqlConnection::_mysqlReady = false;
boost::mutex MySqlConnection::_mysqlShared;


MySqlConnection::MySqlConnection()
    : _mysql(NULL),
      _mysql_res(NULL),
      _isConnected(false),
      _useThreadMgmt(false),
      _isExecuting(false),
      _interrupted(false) {
    _initMySql();
}

MySqlConnection::MySqlConnection(MySqlConfig const& sqlConfig,
                                 bool useThreadMgmt)
    : _mysql(NULL), _mysql_res(NULL),
      _isConnected(false),
      _sqlConfig(boost::make_shared<MySqlConfig>(sqlConfig)),
      _useThreadMgmt(useThreadMgmt),
      _isExecuting(false),
      _interrupted(false) {
    _initMySql();
}

MySqlConnection::~MySqlConnection() {
    if(_mysql) {
        if(_mysql_res) {
            MYSQL_ROW row;
            while((row = mysql_fetch_row(_mysql_res))); // Drain results.
            _mysql_res = NULL;
        }
        killMySql(_mysql, _useThreadMgmt);
    }
}

bool
MySqlConnection::connect() {
    // Cleanup garbage
    if(_mysql) killMySql(_mysql, _useThreadMgmt);
    _isConnected = false;
    // Make myself a thread
    _mysql = _connectHelper();
    _isConnected = (_mysql != NULL);
    return _isConnected;
}

bool
MySqlConnection::queryUnbuffered(std::string const& query) {
    // run query, store into list.
    int rc;
    {
        boost::lock_guard<boost::mutex> lock(_interruptMutex);
        _isExecuting = true;
        _interrupted = false;
    }
    rc = mysql_real_query(_mysql, query.c_str(), query.length());
    if(rc) { return false; }
    _mysql_res = mysql_use_result(_mysql);
    _isExecuting = false;
    if(!_mysql_res) { return false; }
    return true;
}

/// Cancel existing query
/// @return 0 on success.
/// 1 indicates error in connecting. (may try again)
/// 2 indicates error executing kill query. (do not try again)
/// -1 indicates NOP: No query in progress or query already interrupted.
int
MySqlConnection::cancel() {
    boost::lock_guard<boost::mutex> lock(_interruptMutex);
    int rc;
    if(!_isExecuting || _interrupted) {
        // Should we log this?
        return -1; // No further action needed.
    }
    _interrupted = true; // Prevent others from trying to interrupt
    MYSQL* killMysql = _connectHelper();
    if (!killMysql) {
        _interrupted = false; // Didn't try
        return 1;
        // Handle broken connection
    }
    // KILL QUERY only, not KILL CONNECTION.
    int threadId = mysql_thread_id(_mysql);
    std::string killSql = "KILL QUERY " + boost::lexical_cast<int>(threadId);
    rc = mysql_real_query(killMysql, killSql.c_str(), killSql.size());
    mysql_close(killMysql);
    if(rc) {
        return 2;
    }
    return 0;
}

bool
MySqlConnection::selectDb(std::string const& dbName) {
    if(!dbName.empty() &&
       mysql_select_db(_mysql, dbName.c_str())) {
        return false;
    }
    _sqlConfig->dbName = dbName;
    return true;
}

////////////////////////////////////////////////////////////////////////
// MySqlConnection
// private:
////////////////////////////////////////////////////////////////////////
bool
MySqlConnection::_initMySql() {
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

MYSQL* MySqlConnection::_connectHelper() {
    MYSQL* m = mysql_init(NULL);
    if (!m) {
        return m;
    }
    unsigned long clientFlag = CLIENT_MULTI_STATEMENTS;
    MYSQL* c = mysql_real_connect(
        m,
        _sqlConfig->socket.empty() ?_sqlConfig->hostname.c_str() : 0,
        _sqlConfig->username.empty() ? 0 : _sqlConfig->username.c_str(),
        _sqlConfig->password.empty() ? 0 : _sqlConfig->password.c_str(),
        _sqlConfig->dbName.empty() ? 0 : _sqlConfig->dbName.c_str(),
        _sqlConfig->port,
        _sqlConfig->socket.empty() ? 0 : _sqlConfig->socket.c_str(),
        clientFlag);
    if (!c) {
        // Failed to connect: free resources.
        mysql_close(m);
        return c;
    }
    return m;
}

}}} // namespace lsst::qserv::mysql
