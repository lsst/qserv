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
#include <stdexcept>
#include <sstream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.mysql.MySqlConnection");

// A class that calls mysql_thread_end when an instance is destroyed.
struct MySqlThreadJanitor {
    ~MySqlThreadJanitor() { mysql_thread_end(); }
};

// A functor that initializes the MySQL client library.
struct InitializeMysqlLibrary {
    typedef std::unique_ptr<MySqlThreadJanitor> JanitorTsp;
    JanitorTsp& janitor;

    InitializeMysqlLibrary(JanitorTsp& j) : janitor(j) {}

    void operator()() {
        [[maybe_unused]] int rc = mysql_library_init(0, nullptr, nullptr);
        assert(0 == rc && "mysql_library_init() failed");
        assert(mysql_thread_safe() != 0 && "MySQL client library is not thread safe!");
        assert(janitor.get() == 0 && "thread janitor set before calling mysql_library_init()");
        janitor.reset(new MySqlThreadJanitor);
    }
};

/**
 * Establish a new MySQL connection.
 * @param config Parameters of the connection.
 * @return A pointer to the MySQL connection or nullptr.
 */
MYSQL* doConnect(std::shared_ptr<lsst::qserv::mysql::MySqlConfig> const& config) {
    // We must call mysql_library_init() exactly once before calling mysql_init
    // because it is not thread safe. Both mysql_library_init and mysql_init
    // call mysql_thread_init, and so we must arrange to call mysql_thread_end
    // when the calling thread exists. We do this by allocating a thread
    // local object that calls mysql_thread_end from its destructor.
    static std::once_flag initialized;
    static thread_local std::unique_ptr<MySqlThreadJanitor> janitor;

    std::call_once(initialized, InitializeMysqlLibrary(janitor));
    MYSQL* m = mysql_init(nullptr);
    if (nullptr == m) return m;
    if (nullptr == janitor) janitor.reset(new MySqlThreadJanitor);
    unsigned long const clientFlag = CLIENT_MULTI_STATEMENTS;
    mysql_options(m, MYSQL_OPT_LOCAL_INFILE, 0);
    MYSQL* c = mysql_real_connect(m, config->socket.empty() ? config->hostname.c_str() : 0,
                                  config->username.empty() ? 0 : config->username.c_str(),
                                  config->password.empty() ? 0 : config->password.c_str(),
                                  config->dbName.empty() ? 0 : config->dbName.c_str(), config->port,
                                  config->socket.empty() ? 0 : config->socket.c_str(), clientFlag);
    if (nullptr == c) {
        // Failed to connect: free resources.
        mysql_close(m);
        return c;
    }
    return m;
}

}  // namespace

namespace lsst::qserv::mysql {

bool MySqlConnection::checkConnection(mysql::MySqlConfig const& config) {
    MySqlConnection conn(config);
    if (conn.connect()) {
        LOGS(_log, LOG_LVL_DEBUG, "Successful MySQL connection check: " << config);
        return true;
    } else {
        LOGS(_log, LOG_LVL_WARN, "Unsuccessful MySQL connection check: " << config);
        return false;
    }
}

MySqlConnection::MySqlConnection(MySqlConfig const& config)
        : _config(std::make_shared<MySqlConfig>(config)), _mysql(nullptr), _mysql_res(nullptr) {}

MySqlConnection::~MySqlConnection() { _closeMySqlConnImpl(std::lock_guard<std::mutex>(_mtx)); }

void MySqlConnection::closeMySqlConn() { _closeMySqlConnImpl(std::lock_guard<std::mutex>(_mtx)); }

bool MySqlConnection::connect() {
    std::lock_guard<std::mutex> const lock(_mtx);
    _closeMySqlConnImpl(lock);
    _mysql = ::doConnect(_config);
    if (nullptr != _mysql) {
        _threadId = mysql_thread_id(_mysql);
        return true;
    }
    return false;
}

bool MySqlConnection::queryUnbuffered(std::string const& query) {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_mysql == nullptr) return false;
    int const rc = mysql_real_query(_mysql, query.c_str(), query.length());
    if (rc) return false;
    _mysql_res = mysql_use_result(_mysql);
    if (nullptr == _mysql_res) return false;
    return true;
}

MySqlConnection::CancelStatus MySqlConnection::cancel() {
    unsigned int const threadId = _threadId.load();
    if (!(connected() && (0 != threadId))) return CancelStatus::CANCEL_NOP;
    MYSQL* killMysql = ::doConnect(_config);
    if (nullptr == killMysql) return CancelStatus::CANCEL_CONNECT_ERROR;
    std::string const killSql = "KILL QUERY " + std::to_string(threadId);
    int const rc = mysql_real_query(killMysql, killSql.c_str(), killSql.size());
    mysql_close(killMysql);
    if (rc) {
        LOGS(_log, LOG_LVL_WARN,
             "failed to kill MySQL thread: " << threadId << ", error: " << std::string(mysql_error(killMysql))
                                             << ", errno: " << std::to_string(mysql_errno(killMysql)));
        return CancelStatus::CANCEL_FAILED;
    }
    return CancelStatus::CANCEL_SUCCESS;
}

MYSQL* MySqlConnection::getMySql() {
    _throwIfNotConnected(__func__);
    return _mysql;
}

MYSQL_RES* MySqlConnection::getResult() {
    _throwIfNotConnected(__func__);
    return _mysql_res;
}

void MySqlConnection::freeResult() {
    std::lock_guard<std::mutex> lock(_mtx);
    _throwIfNotInProcessingResult(__func__);
    mysql_free_result(_mysql_res);
    _mysql_res = nullptr;
}

int MySqlConnection::getResultFieldCount() {
    std::lock_guard<std::mutex> lock(_mtx);
    _throwIfNotInProcessingResult(__func__);
    return mysql_field_count(_mysql);
}

std::vector<std::string> MySqlConnection::getColumnNames() const {
    std::lock_guard<std::mutex> lock(_mtx);
    _throwIfNotInProcessingResult(__func__);
    std::vector<std::string> names;
    if (0 != mysql_field_count(_mysql)) {
        auto fields = mysql_fetch_fields(_mysql_res);
        for (unsigned int i = 0; i < mysql_num_fields(_mysql_res); i++) {
            names.push_back(std::string(fields[i].name));
        }
    }
    return names;
}

unsigned int MySqlConnection::getErrno() const {
    _throwIfNotConnected(__func__);
    return mysql_errno(_mysql);
}
const std::string MySqlConnection::getError() const {
    _throwIfNotConnected(__func__);
    return std::string(mysql_error(_mysql));
}

bool MySqlConnection::selectDb(std::string const& dbName) {
    _throwIfNotConnected(__func__);
    if (!dbName.empty() && (0 != mysql_select_db(_mysql, dbName.c_str()))) {
        return false;
    }
    _config->dbName = dbName;
    return true;
}

std::string MySqlConnection::dump() {
    std::ostringstream os;
    os << "hostN=" << _config->hostname << " sock=" << _config->socket << " uname=" << _config->username
       << " dbN=" << _config->dbName << " port=" << _config->port;
    return os.str();
}

void MySqlConnection::_closeMySqlConnImpl(std::lock_guard<std::mutex> const& lock) {
    if (nullptr != _mysql) {
        mysql_close(_mysql);
        _mysql = nullptr;
        _threadId = 0;
        if (nullptr != _mysql_res) {
            mysql_free_result(_mysql_res);
            _mysql_res = nullptr;
        }
    }
}

void MySqlConnection::_throwIfNotConnected(std::string const& func) const {
    if (_mysql == nullptr) {
        throw std::logic_error("MySqlConnection::" + func + " connection is not open.");
    }
}

void MySqlConnection::_throwIfNotInProcessingResult(std::string const& func) const {
    _throwIfNotConnected(func);
    if (_mysql_res == nullptr) {
        throw std::logic_error("MySqlConnection::" + func + " not in the result processing context.");
    }
}

}  // namespace lsst::qserv::mysql
