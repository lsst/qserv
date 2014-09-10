// -*- LSST-C++ -*-
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
// MySqlConnection represents an abstracted interface to the mysql C-API.
// Each MySqlConnection object is not parallel, but multiple objects can be used
// to achieve parallel query streams.

#ifndef LSST_QSERV_MYSQL_MYSQLCONNECTION_H
#define LSST_QSERV_MYSQL_MYSQLCONNECTION_H

// Third-party headers
#include <boost/thread.hpp>
#include <boost/utility.hpp>

// Local headers
#include <mysql/mysql.h>


namespace lsst {
namespace qserv {
namespace mysql {

// Forward
class MySqlConfig;

/// MySqlConnection is a thin wrapper around the MySQL C-API that partially
/// shields clients from the raw API, while still providing raw access for
/// clients that need it.
class MySqlConnection : boost::noncopyable {
public:
    MySqlConnection();
    MySqlConnection(MySqlConfig const& sqlConfig, bool useThreadMgmt=false);

    ~MySqlConnection();

    bool connect();

    bool connected() const { return _isConnected; }
    // instance destruction invalidates this return value
    MYSQL* getMySql() { return _mysql;}
    MySqlConfig const& getMySqlConfig() const { return *_sqlConfig; }

    bool queryUnbuffered(std::string const& query);
    MYSQL_RES* getResult() { return _mysql_res; }
    void freeResult() { mysql_free_result(_mysql_res); _mysql_res = NULL; }
    int getResultFieldCount() {
        assert(_mysql);
        return mysql_field_count(_mysql);
    }
    MySqlConfig const& getConfig() const { return *_sqlConfig; }
    bool selectDb(std::string const& dbName);


private:
    bool _initMySql();
    static boost::mutex _mysqlShared;
    static bool _mysqlReady;

    MYSQL* _mysql;
    MYSQL_RES* _mysql_res;
    bool _isConnected;
    boost::shared_ptr<MySqlConfig> _sqlConfig;
    bool _useThreadMgmt;
};

}}} // namespace lsst::qserv::mysql

#endif // LSST_QSERV_MYSQL_MYSQLCONNECTION_H

