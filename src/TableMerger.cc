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
 
#include <sys/time.h> 
#include <sstream>
#include <iostream>
#include <boost/format.hpp>
#include "lsst/qserv/master/TableMerger.h"
#include "lsst/qserv/master/sql.h"
using lsst::qserv::master::SqlConfig;
using lsst::qserv::master::SqlConnection;
using lsst::qserv::master::TableMerger;
using lsst::qserv::master::TableMergerError;
using lsst::qserv::master::TableMergerConfig;

namespace { // File-scope helpers

std::string getTimeStampId() {
    struct timeval now;
    int rc = gettimeofday(&now, NULL);
    assert(rc == 0);
    std::stringstream s;
    s << (now.tv_sec % 10000) << now.tv_usec;
    return s.str();
    // Use the lower digits as pseudo-unique (usec, sec % 10000)
    // FIXME: is there a better idea?
}

boost::shared_ptr<SqlConfig> makeSqlConfig(TableMergerConfig const& c) {
    boost::shared_ptr<SqlConfig> sc(new SqlConfig());
    assert(sc.get());
    sc->username = c.user;
    sc->dbName = c.targetDb;
    sc->socket = c.socket;
    return sc;
}
    
} // anonymous namespace

std::string const TableMerger::_dropSql("DROP TABLE IF EXISTS %s;");
std::string const TableMerger::_createSql("CREATE TABLE IF NOT EXISTS %s SELECT * FROM %s;");
std::string const TableMerger::_createFixSql("CREATE TABLE IF NOT EXISTS %s SELECT %s FROM %s %s;");
std::string const TableMerger::_insertSql("INSERT INTO %s SELECT * FROM %s;");
std::string const TableMerger::_cleanupSql("DROP TABLE IF EXISTS %s;");
std::string const TableMerger::_cmdBase("%1% --socket=%2% -u %3% %4%");


////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////
TableMerger::TableMerger(TableMergerConfig const& c) 
    : _config(c),
      _sqlConfig(makeSqlConfig(c)),
      _tableCount(0) {
    _fixupTargetName();
    _loadCmd = (boost::format(_cmdBase)
		% c.mySqlCmd % c.socket % c.user % c.targetDb).str();    
}

bool TableMerger::merge(std::string const& dumpFile, 
			std::string const& tableName) {
    bool isOk = true;
    std::string sql;
    _importResult(dumpFile); 
    {
        //std::cout << "Importing " << tableName << std::endl;
	boost::lock_guard<boost::mutex> g(_countMutex);
	++_tableCount;
	if(_tableCount == 1) {
	    sql = _buildMergeSql(tableName, true);
            isOk = _applySql(sql);
            if(!isOk) {
                std::cout << "Failed importing! " << tableName 
                          << " " << _error.description << std::endl;
                --_tableCount; // We failed merging the table.
            }
	    return isOk; // must happen first.
	}
    }
    // No locking needed if not first, after updating the counter.
    sql = _buildMergeSql(tableName, false); 
    return _applySql(sql);
}

bool TableMerger::finalize() {
    if(_mergeTable != _config.targetTable) {
	std::string cleanup = (boost::format(_cleanupSql) % _mergeTable).str();
        std::string fixupSuffix = _config.mFixup.post + _buildOrderByLimit();


	// Need to perform fixup for aggregation.
	std::string sql = (boost::format(_createFixSql) 
			   % _config.targetTable 
			   % _config.mFixup.select
			   % _mergeTable 
			   % fixupSuffix).str() + cleanup;
	return _applySql(sql);
    }
    return true;
}
////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////
bool TableMerger::_applySql(std::string const& sql) {
    return _applySqlLocal(sql); //try local impl now.
    FILE* fp;
    {
	boost::lock_guard<boost::mutex> m(_popenMutex);
	fp = popen(_loadCmd.c_str(), "w"); // check error
    }
    if(fp == NULL) {
	_error.status = TableMergerError::MYSQLOPEN;
	_error.errorCode = 0;
	_error.description = "Error starting mysql process.";
	return false;
    }
    int written = fwrite(sql.c_str(), sizeof(std::string::value_type), 
                         sql.size(), fp);
    if(written != (sql.size()*sizeof(std::string::value_type))) {
	_error.status = TableMergerError::MERGEWRITE;
	_error.errorCode = written;
	_error.description = "Error writing sql to mysql process.." + sql;
	{
	    boost::lock_guard<boost::mutex> m(_popenMutex);
	    pclose(fp); // cleanup
	}
	return false;
    }
    int r;
    {
	boost::lock_guard<boost::mutex> m(_popenMutex);
	r = pclose(fp);
    }
    if(r == -1) {
	_error.status = TableMergerError::TERMINATE;
	_error.errorCode = r;
	_error.description = "Error finalizing merge step..";
	return false;
    }
    return true;
}

bool TableMerger::_applySqlLocal(std::string const& sql) {
    boost::lock_guard<boost::mutex> m(_sqlMutex);
    if(!_sqlConn.get()) {
        _sqlConn.reset(new SqlConnection(*_sqlConfig));
        if(!_sqlConn->connectToDb()) {
            std::stringstream ss;
            _error.status = TableMergerError::MYSQLCONNECT;
            _error.errorCode = _sqlConn->getMySqlErrno();
            ss << "Code:" << _error.errorCode << " "
               << _sqlConn->getMySqlError();
            _error.description = "Error connecting to db." + ss.str();
            _sqlConn.reset();
            return false;
        }
    }
    if(!_sqlConn->apply(sql)) {
	std::stringstream ss;
	_error.status = TableMergerError::MYSQLEXEC;
	_error.errorCode = _sqlConn->getMySqlErrno();
	ss << "Code:" << _error.errorCode << " "
	   << _sqlConn->getMySqlError();
	_error.description = "Error applying sql." + ss.str();
	return false;
    }

    return true;
}

std::string TableMerger::_buildMergeSql(std::string const& tableName, 
					bool create) {
    std::string cleanup = (boost::format(_cleanupSql) % tableName).str();
    
    if(create) {
	return (boost::format(_dropSql) % _mergeTable).str() 
	    + (boost::format(_createSql) % _mergeTable 
	       % tableName).str() + cleanup;
    } else {
	return (boost::format(_insertSql) %  _mergeTable 
		% tableName).str() + cleanup;
    }
}

std::string TableMerger::_buildOrderByLimit() {
    std::stringstream ss;
    if(!_config.mFixup.orderBy.empty()) {
        ss << "ORDER BY " << _config.mFixup.orderBy;
    }
    if(_config.mFixup.limit != -1) {
        if(!_config.mFixup.orderBy.empty()) { ss << " "; }
        ss << "LIMIT " << _config.mFixup.limit;
    }
    return ss.str();
}

void TableMerger::_fixupTargetName() {
    if(_config.targetTable.empty()) {
	assert(!_config.targetDb.empty());
	_config.targetTable = (boost::format("%1%.result_%2%") 
			       % _config.targetDb % getTimeStampId()).str();
    }
    
    if(_config.mFixup.needsFixup) {
	// Set merging temporary if needed.
	_mergeTable = _config.targetTable + "_m"; 
    } else {
	_mergeTable = _config.targetTable;
    }
}

bool TableMerger::_importResult(std::string const& dumpFile) {
    int rc = system((_loadCmd + "<" + dumpFile).c_str());
    if(rc != 0) {
	_error.status = TableMergerError::IMPORT;
	_error.errorCode = rc;
	_error.description = "Error importing result db.";
	return false;	
    }
    return true;
}

