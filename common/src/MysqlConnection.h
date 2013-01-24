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
// X is a ...

#ifndef LSST_QSERV_MYSQLCONNECTION_H
#define LSST_QSERV_MYSQLCONNECTION_H
#include <mysql/mysql.h>
// Boost
#include <boost/thread.hpp>

namespace lsst { namespace qserv {
// Forward
class SqlConfig;

class MysqlConnection {
public:
    MysqlConnection();
    MysqlConnection(SqlConfig const& sqlConfig, bool useThreadMgmt=false);
    
    ~MysqlConnection();
    
    bool connect();

    bool connected() const { return _isConnected; }
    // instance destruction invalidates this return value
    MYSQL* getMysql() { return _mysql;} 
    SqlConfig const& getSqlConfig() const { return *_sqlConfig; }
    
    bool queryUnbuffered(std::string const& query);
    MYSQL_RES* getResult() { return _mysql_res; }
    void freeResult() { mysql_free_result(_mysql_res); _mysql_res = NULL; }
    int getResultFieldCount() {
        assert(_mysql);
        return mysql_field_count(_mysql);
    }
private:
    bool _initMysql();
    static boost::mutex _mysqlShared;
    static bool _mysqlReady;

    MYSQL* _mysql;
    MYSQL_RES* _mysql_res;
    bool _isConnected;
    boost::shared_ptr<SqlConfig> _sqlConfig;
    bool _useThreadMgmt;
};

}} // namespace lsst::qserv


#endif // LSST_QSERV_MYSQLCONNECTION_H

