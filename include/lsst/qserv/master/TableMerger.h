#ifndef LSST_QSERV_MASTER_TABLE_MERGER_H
#define LSST_QSERV_MASTER_TABLE_MERGER_H
#include <string>

namespace lsst {
namespace qserv {
namespace master {

struct TableMergerError {
public:
    enum {NONE, IMPORT, MYSQLOPEN, MERGEWRITE, TERMINATE} status;
    int errorCode;
    std::string description;
};

class TableMergerConfig {
public:
    TableMergerConfig(std::string targetDb_, std::string targetTable_,
	   std::string user_, std::string socket_,
	   std::string mySqlCmd_) 
	:  targetDb(targetDb_),  targetTable(targetTable_),
	   user(user_),  socket(socket_), mySqlCmd(mySqlCmd_)  
    {}

    std::string targetDb; // for final result, and imported result
    std::string targetTable;
    std::string user;
    std::string socket;
    std::string mySqlCmd;
};


class TableMerger {
public:
    // Workaround. SWIG doesn't support nested classes.
    //typedef TableMergerError Error;
    //typedef TableMergerConfig Config;

    TableMerger(TableMergerConfig const& c);
    bool merge(std::string const& dumpFile, std::string const& tableName);
    
    TableMergerError getError() { return _error; }

private:
    std::string _buildSql(std::string const& tableName);
    bool _importResult(std::string const& dumpFile);

    static std::string const _dropSql;
    static std::string const _createSql;
    static std::string const _insertSql;
    static std::string const _cleanupSql;
    static std::string const _cmdBase;

    std::string _loadCmd;
    TableMergerConfig _config;
    TableMergerError _error;
    long long _resultLimit;
    int _tableCount;
	
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_TABLE_MERGER_H
