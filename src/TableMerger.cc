#include <sys/time.h> 
#include <sstream>
#include <boost/format.hpp>
#include "lsst/qserv/master/TableMerger.h"
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

} // anonymous namespace

std::string const TableMerger::_dropSql("DROP TABLE IF EXISTS %s;");
std::string const TableMerger::_createSql("CREATE TABLE %s SELECT * FROM %s;");
std::string const TableMerger::_insertSql("INSERT INTO %s SELECT * FROM %s;");
std::string const TableMerger::_cleanupSql("DROP TABLE %s;");
std::string const TableMerger::_cmdBase("%1% --socket=%2% -u %3% %4%");

TableMerger::TableMerger(TableMergerConfig const& c) 
    : _config(c), _tableCount(0) {
    _fixupTargetName();
    _loadCmd = (boost::format(_cmdBase)
		% c.mySqlCmd % c.socket % c.user % c.targetDb).str();    
}

bool TableMerger::merge(std::string const& dumpFile, 
			std::string const& tableName) {
    _importResult(dumpFile);
    std::string sql = _buildSql(tableName);
    FILE* fp;
    fp = popen(_loadCmd.c_str(), "w"); // check error
    if(fp == NULL) {
	_error.status = TableMergerError::MYSQLOPEN;
	_error.errorCode = 0;
	_error.description = "Error starting mysql process.";
	return false;
    }
    int written = fwrite(sql.c_str(), sql.size(), 
			 sizeof(std::string::value_type), fp);
    if(written != (sql.size()*sizeof(std::string::value_type))) {
	_error.status = TableMergerError::MERGEWRITE;
	_error.errorCode = written;
	_error.description = "Error writing sql to mysql process..";
	pclose(fp); // cleanup
	return false;
    }
    int r = pclose(fp);
    if(r == -1) {
	_error.status = TableMergerError::TERMINATE;
	_error.errorCode = r;
	_error.description = "Error finalizing merge step..";
	return false;
    }
}

std::string TableMerger::_buildSql(std::string const& tableName) {
    std::string cleanup = (boost::format(_cleanupSql) % tableName).str();

    if(_tableCount == 0) {
	return (boost::format(_dropSql) % _config.targetTable).str() 
	    + (boost::format(_createSql) % _config.targetTable 
	       % tableName).str() + cleanup;
    } else {
	return (boost::format(_insertSql) %  _config.targetTable 
		% tableName).str() + cleanup;
    }
}

void TableMerger::_fixupTargetName() {
    if(_config.targetTable.empty()) {
	assert(!_config.targetDb.empty());
	_config.targetTable = (boost::format("%1%.result_%2%") 
			       % _config.targetDb % getTimeStampId()).str();
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

