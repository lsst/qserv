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

#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdio>
// Boost
#include <boost/thread.hpp> // for mutex.
#include <boost/format.hpp>

// mysql
#include "sql/SqlConnection.h"
#include "mysql/MySqlConnection.h"
#include "sql/SqlResults.h"

using namespace lsst::qserv;

/*
SqlConfig::SqlConfig(const SqlConfig& c)
    : hostname(c.hostname),
      username(c.username),
      password(c.password),
      dbName(c.dbName),
      port(c.port),
      socket(c.socket) {
}

void
SqlConfig::throwIfNotSet(std::string const& fName) const {
    bool allSet = true;
    std::stringstream s;
    s << "Value for ";
    if (hostname == "") { allSet = false; s << "host "; }
    if (port     == 0 ) { allSet = false; s << "port "; }
    if (username == "") { allSet = false; s << "username "; }
    if (password == "") { allSet = false; s << "password "; }
    if (dbName   == "") { allSet = false; s << "dbName ";   }
    if (socket   == "") { allSet = false; s << "socket "; }
    if (!allSet) {
        s << "not set in the '" << fName << "' file.";
        throw s.str();
    }
}

/// Initializes self from a file. File format: <key>:<value>
/// To ignore given token, pass "".
/// To ignore unrecognized tokens, set the flag to false.
/// This is handy for reading a subset of a file.
void
SqlConfig::initFromFile(std::string const& fName,
                        std::string const& hostToken,
                        std::string const& portToken,
                        std::string const& userToken,
                        std::string const& passToken,
                        std::string const& dbNmToken,
                        std::string const& sockToken,
                        bool ignoreUnrecognizedTokens) {
    std::ifstream f;
    f.open(fName.c_str());
    if (!f) {
        std::stringstream s;
        s << "Failed to open '" << fName << "'";
        throw s.str();
    }
    std::string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            std::stringstream s;
            s << "Invalid format, expecting <token>:<value>. "
              << "File '" << fName << "', line: '" << line << "'";
            throw s.str();
        }
        std::string token = line.substr(0,pos);
        std::string value = line.substr(pos+1, line.size());
        if (hostToken != "" and token == hostToken) {
            this->hostname = value;
        } else if (portToken != "" and token == portToken) {
            this->port = atoi(value.c_str());
            if ( this->port <= 0 ) {
                std::stringstream s;
                s << "Invalid port number " << this->port << ". "
                  << "File '" << fName << "', line: '" << line << "'";
                throw s.str();
            }
        } else if (userToken != "" and token == userToken) {
            this->username = value;
        } else if (passToken != "" and token == passToken) {
            this->password = value;
        } else if (dbNmToken != "" and token == dbNmToken) {
            this->dbName = value;
        } else if (sockToken != "" and token == sockToken) {
            this->socket = value;
        } else if (!ignoreUnrecognizedTokens) {
            std::stringstream s;
            s << "Unexpected token: '" << token << "' (supported tokens "
              << "are: " << hostToken << ", " << portToken << ", "
              << userToken << ", " << passToken << ", " << dbNmToken << ", "
              << sockToken << ").";
            throw(s.str());
        }
        f >> line;
    }
    f.close();
    //throwIfNotSet(fName);
}

void
SqlConfig::printSelf(std::string const& extras) const {
    std::cout << "(" << extras << ") host=" << hostname << ", port=" << port
              << ", usr=" << username << ", pass=" << password << ", dbName="
              << dbName << ", socket=" << socket << std::endl;
}
*/

////////////////////////////////////////////////////////////////////////
// class SqlResultIter
////////////////////////////////////////////////////////////////////////
SqlResultIter::SqlResultIter(SqlConfig const& sqlConfig,
                             std::string const& query) {
    if(!_setup(sqlConfig, query)) { return; }
    // if not error, prime the iterator
    ++(*this);
}

SqlResultIter&
SqlResultIter::operator++() {
    MYSQL_RES* result = _connection->getResult();
    if(!_columnCount) {
        _columnCount = mysql_num_fields(result);
        _current.resize(_columnCount);
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    if(!row) {
        _connection->freeResult();
        return *this;
    }
    std::copy(row, row + _columnCount, _current.begin());
    return *this; // FIXME
}

bool
SqlResultIter::done() const {
    // done if result is null, because connection has freed its result.
    return !_connection->getResult();
}

bool
SqlResultIter::_setup(SqlConfig const& sqlConfig, std::string const& query) {
    _columnCount = 0;
    _connection.reset(new MySqlConnection(sqlConfig, true));
    if(!_connection->connect()) {
        SqlConnection::populateErrorObject(*_connection, _errObj);
        return false;
    }
    if(!_connection->queryUnbuffered(query)) {
        SqlConnection::populateErrorObject(*_connection, _errObj);
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////
// class SqlConnection
////////////////////////////////////////////////////////////////////////
SqlConnection::SqlConnection()
    : _connection() {
}

SqlConnection::SqlConnection(SqlConfig const& sc, bool useThreadMgmt)
    : _connection(new MySqlConnection(sc, useThreadMgmt)) {
}

void
SqlConnection::reset(SqlConfig const& sc, bool useThreadMgmt) {
    _connection.reset(new MySqlConnection(sc, useThreadMgmt));
}

SqlConnection::~SqlConnection() {
    _connection.reset();
}

bool
SqlConnection::connectToDb(SqlErrorObject& errObj) {
    assert(_connection.get());
    if(_connection->connected()) return true;
    if(!_connection->connect()) {
        _setErrorObject(errObj);
        return false;
    } else {
        return true;
    }
}

bool
SqlConnection::selectDb(std::string const& dbName, SqlErrorObject& errObj) {
    if (!connectToDb(errObj)) return false;
    if (_connection->getConfig().dbName == dbName) {
        return true; // nothing to do
    }
    if (!dbExists(dbName, errObj)) {
        return errObj.addErrMsg(std::string("Can't switch to db ")
                                 + dbName + " (it does not exist).");
    }
    if(!_connection->selectDb(dbName)) {
        _setErrorObject(errObj, "Problem selecting db " + dbName + ".");
        return false;
    }
    return true;
}

bool
SqlConnection::runQuery(char const* query,
                        int qSize,
                        SqlResults& results,
                        SqlErrorObject& errObj) {
    std::string queryPiece(query, qSize);
    if (!connectToDb(errObj)) return false;
    if (mysql_real_query(_connection->getMySql(), query, qSize) != 0) {
        MYSQL_RES* result = mysql_store_result(_connection->getMySql());
        if (result) mysql_free_result(result);
        std::string msg = std::string("Unable to execute query: ")
            + queryPiece;
        //    + "\nQuery = " + std::string(query, qSize);
        return _setErrorObject(errObj, msg);
    }
    int status = 0;
    do {
        MYSQL_RES* result = mysql_store_result(_connection->getMySql());
        if (result) {
            results.addResult(result);
        } else if (mysql_field_count(_connection->getMySql()) != 0) {
            return _setErrorObject(errObj,
                    std::string("Unable to store result for query: ") + queryPiece);
        }
        status = mysql_next_result(_connection->getMySql());
        if (status > 0) {
            return _setErrorObject(errObj,
                  std::string("Error retrieving results for query: ") + queryPiece);
        }
    } while (status == 0);
    return true;
}

bool
SqlConnection::runQuery(char const* query, int qSize, SqlErrorObject& errObj) {
    SqlResults results(true); // true - discard results immediately
    return runQuery(query, qSize, results, errObj);
}

bool
SqlConnection::runQuery(std::string const query,
                        SqlResults& results,
                        SqlErrorObject& errObj) {
    return runQuery(query.data(), query.size(), results, errObj);
}

bool
SqlConnection::runQuery(std::string const query,
                        SqlErrorObject& errObj) {
    SqlResults results(true); // true - discard results immediately
    return runQuery(query.data(), query.size(), results, errObj);
}

/// with runQueryIter SqlConnection is busy until SqlResultIter is closed
boost::shared_ptr<SqlResultIter>
SqlConnection::getQueryIter(std::string const& query) {
    boost::shared_ptr<SqlResultIter> i(
        new SqlResultIter(_connection->getConfig(), query));
    return i; // Can't defer to iterator without thread mgmt.
}

bool
SqlConnection::dbExists(std::string const& dbName, SqlErrorObject& errObj) {
    if (!connectToDb(errObj)) return false;
    std::string sql = "SELECT COUNT(*) FROM information_schema.schemata ";
    sql += "WHERE schema_name = '";
    sql += dbName + "'";

    SqlResults results;
    if ( !runQuery(sql, results, errObj) ) {
        return errObj.addErrMsg("Failed to run: " + sql);
    }
    std::string s;
    if ( !results.extractFirstValue(s, errObj)) {
        return false;
    }
    return s[0] == '1';
}

bool
SqlConnection::createDb(std::string const& dbName,
                        SqlErrorObject& errObj,
                        bool failIfExists) {
    if (!connectToDb(errObj)) return false;
    if (dbExists(dbName, errObj)) {
        if (failIfExists) {
            return errObj.addErrMsg(std::string("Can't create db ")
                                    + dbName + ", it already exists");
        }
        return true;
    } else {
        if ( errObj.isSet() ) { return false; } // Can't check existence.
    }
    std::string sql = "CREATE DATABASE " + dbName;
    if (!runQuery(sql, errObj)) {
        return _setErrorObject(errObj, "Problem executing: " + sql);
    }
    return true;
}

bool
SqlConnection::createDbAndSelect(std::string const& dbName,
                                 SqlErrorObject& errObj,
                                 bool failIfExists) {
    if ( ! createDb(dbName, errObj, failIfExists) ) {
        return false;
    }
    return selectDb(dbName, errObj);
}

bool
SqlConnection::dropDb(std::string const& dbName,
                      SqlErrorObject& errObj,
                      bool failIfDoesNotExist) {
    if (!connectToDb(errObj)) return false;
    if (!dbExists(dbName, errObj)) {
        if ( errObj.isSet() ) { return false; } // Can't check existence.
        if ( failIfDoesNotExist ) {
            return errObj.addErrMsg(std::string("Can't drop db ")
                                    + dbName + ", it does not exist");
        }
        return true;
    }
    std::string sql = "DROP DATABASE " + dbName;
    if (!runQuery(sql, errObj)) {
        return _setErrorObject(errObj, "Problem executing: " + sql);
    }
    if ( getActiveDbName() == dbName ) {
        _connection->selectDb(std::string());

    }
    return true;
}

bool
SqlConnection::tableExists(std::string const& tableName,
                           SqlErrorObject& errObj,
                           std::string const& dbName) {
    if (!connectToDb(errObj)) return false;
    std::string dbName_;
    if ( ! dbName.empty() ) {
        dbName_ = dbName;
    } else {
        dbName_ = getActiveDbName();
        if (dbName_.empty() ) {
            return errObj.addErrMsg(
                            "Can't check if table existd, db not selected. ");
        }
    }
    if (!dbExists(dbName_, errObj)) {
        return errObj.addErrMsg(std::string("Db ")+dbName_+" does not exist");
    }
    std::string sql = "SELECT COUNT(*) FROM information_schema.tables ";
    sql += "WHERE table_schema = '";
    sql += dbName_ + "' AND table_name = '" + tableName + "'";
    SqlResults results;
    if (!runQuery(sql, results, errObj)) {
        return _setErrorObject(errObj, "Problem executing: " + sql);
    }
    std::string s;
    if ( !results.extractFirstValue(s, errObj) ) {
        return errObj.addErrMsg("Query " + sql + " did not return result");
    }
    return s[0] == '1';
}

bool
SqlConnection::dropTable(std::string const& tableName,
                         SqlErrorObject& errObj,
                         bool failIfDoesNotExist,
                         std::string const& dbName) {
    if (!connectToDb(errObj)) return false;
    if ( getActiveDbName().empty() ) {
        return errObj.addErrMsg("Can't drop table, db not selected");
    }
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!tableExists(tableName, errObj, _dbName)) {
        if (failIfDoesNotExist) {
            return errObj.addErrMsg(std::string("Can't drop table ")
                                    + tableName + " (does not exist)");
        }
        return true;
    }
    std::string sql = "DROP TABLE " + _dbName + "." + tableName;
    if (!runQuery(sql, errObj)) {
        return _setErrorObject(errObj, "Problem executing: " + sql);
    }
    return true;
}

bool
SqlConnection::listTables(std::vector<std::string>& v,
                          SqlErrorObject& errObj,
                          std::string const& prefixed,
                          std::string const& dbName) {
    v.clear();
    if (!connectToDb(errObj)) return false;
    if ( getActiveDbName().empty() ) {
        return errObj.addErrMsg("Can't list tables, db not selected. ");
    }
    std::string _dbName = (dbName == "" ? getActiveDbName() : dbName);
    if (!dbExists(_dbName, errObj)) {
        return errObj.addErrMsg("Can't list tables for db " + _dbName
                                + " because the database does not exist. ");
    }
    std::string sql = "SELECT table_name FROM information_schema.tables ";
    sql += "WHERE table_schema = '" + _dbName + "'";
    if (prefixed != "") {
        sql += " AND table_name LIKE '" + prefixed + "%'";
    }
    SqlResults results;
    if (!runQuery(sql, results, errObj)) {
        return _setErrorObject(errObj, "Problem executing: " + sql);
    }
    return results.extractFirstColumn(v, errObj);
}

std::string
SqlConnection::getActiveDbName() const {
    return _connection->getConfig().dbName;
}

void
SqlConnection::populateErrorObject(MySqlConnection& m, SqlErrorObject& o) {
    MYSQL* mysql = m.getMySql();
    if(mysql == NULL) {
        o.setErrNo(-999);
    } else {
        o.setErrNo( mysql_errno(mysql) );
        o.addErrMsg( mysql_error(mysql) );
    }
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////

bool
SqlConnection::_setErrorObject(SqlErrorObject& errObj,
                               std::string const& extraMsg) {
    assert(_connection.get());
    populateErrorObject(*_connection, errObj);
    if ( ! extraMsg.empty() ) {
        errObj.addErrMsg(extraMsg);
    }
    return false;
}
