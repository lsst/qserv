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

// System headers
#include <cstddef>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.mysql.MySqlConnection");

} // anonymous


namespace {
    // A class that calls mysql_thread_end when an instance is destroyed.
    struct MySqlThreadJanitor {
        ~MySqlThreadJanitor() { mysql_thread_end(); }
    };

    // A functor that initializes the MySQL client library.
    struct InitializeMysqlLibrary {
        typedef std::unique_ptr<MySqlThreadJanitor> JanitorTsp;
        JanitorTsp & janitor;

        InitializeMysqlLibrary(JanitorTsp & j) : janitor(j) {}

        void operator()() {
            int rc = mysql_library_init(0, nullptr, nullptr);
            assert(0 == rc && "mysql_library_init() failed");
            assert(mysql_thread_safe() != 0 &&
                   "MySQL client library is not thread safe!");
            assert(janitor.get() == 0 &&
                   "thread janitor set before calling mysql_library_init()");
            janitor.reset(new MySqlThreadJanitor);
        }
    };
}


namespace lsst {
namespace qserv {
namespace mysql {

MySqlConnection::MySqlConnection()
    : _mysql(nullptr),
      _mysql_res(nullptr),
      _isConnected(false),
      _isExecuting(false),
      _interrupted(false) {
}

MySqlConnection::MySqlConnection(MySqlConfig const& sqlConfig)
    : _mysql(nullptr),
      _mysql_res(nullptr),
      _isConnected(false),
      _sqlConfig(std::make_shared<MySqlConfig>(sqlConfig)),
      _isExecuting(false),
      _interrupted(false) {
}

MySqlConnection::~MySqlConnection() {
    if (_mysql) {
        if (_mysql_res) {
            MYSQL_ROW row;
            while((row = mysql_fetch_row(_mysql_res))); // Drain results.
            _mysql_res = nullptr;
        }
        closeMySqlConn();
    }
}

bool MySqlConnection::checkConnection(mysql::MySqlConfig const& mysqlconfig) {
    MySqlConnection conn(mysqlconfig);
    if (conn.connect()) {
        LOGS(_log, LOG_LVL_DEBUG, "Successful MySQL connection check: " << mysqlconfig);
        return true;
    } else {
        LOGS(_log, LOG_LVL_WARN, "Unsuccessful MySQL connection check: " << mysqlconfig);
        return false;
    }
}

void
MySqlConnection::closeMySqlConn() {
    // Close mysql connection and set deallocated pointer to null
    mysql_close(_mysql);
    _mysql = nullptr;
}

bool
MySqlConnection::connect() {
    // Cleanup garbage
    if (_mysql != nullptr) { closeMySqlConn(); }
    _isConnected = false;
    // Make myself a thread
    _mysql = _connectHelper();
    _isConnected = (_mysql != nullptr);
    return _isConnected;
}

bool
MySqlConnection::queryUnbuffered(std::string const& query) {
    // run query, store into list.
    int rc;
    {
        std::lock_guard<std::mutex> lock(_interruptMutex);
        _isExecuting = true;
        _interrupted = false;
    }
    rc = mysql_real_query(_mysql, query.c_str(), query.length());
    if (rc) { return false; }
    _mysql_res = mysql_use_result(_mysql);
    _isExecuting = false;
    if (!_mysql_res) { return false; }
    return true;
}

/// Cancel existing query
/// @return 0 on success.
/// 1 indicates error in connecting. (may try again)
/// 2 indicates error executing kill query. (do not try again)
/// -1 indicates NOP: No query in progress or query already interrupted.
int
MySqlConnection::cancel() {
    std::lock_guard<std::mutex> lock(_interruptMutex);
    int rc;
    if (!_isExecuting || _interrupted) {
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
    std::string const killSql = "KILL QUERY " + std::to_string(threadId);
    rc = mysql_real_query(killMysql, killSql.c_str(), killSql.size());
    mysql_close(killMysql);
    if (rc) {
        return 2;
    }
    return 0;
}

bool
MySqlConnection::selectDb(std::string const& dbName) {
    if (!dbName.empty() &&
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

MYSQL* MySqlConnection::_connectHelper() {
    // We must call mysql_library_init() exactly once before calling mysql_init
    // because it is not thread safe. Both mysql_library_init and mysql_init
    // call mysql_thread_init, and so we must arrange to call mysql_thread_end
    // when the calling thread exists. We do this by allocating a thread
    // local object that calls mysql_thread_end from its destructor.
    static std::once_flag initialized;
    static thread_local std::unique_ptr<MySqlThreadJanitor> janitor;

    std::call_once(initialized, InitializeMysqlLibrary(janitor));
    MYSQL* m = mysql_init(nullptr);
    if (!m) {
        return m;
    }
    if (!janitor.get()) {
        janitor.reset(new MySqlThreadJanitor);
    }
    unsigned long clientFlag = CLIENT_MULTI_STATEMENTS;
    mysql_options( m, MYSQL_OPT_LOCAL_INFILE, 0 );
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
