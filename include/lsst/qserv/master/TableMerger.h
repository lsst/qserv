#ifndef LSST_QSERV_MASTER_TABLE_MERGER_H
#define LSST_QSERV_MASTER_TABLE_MERGER_H
#include <string>
#include <boost/thread.hpp> // for mutex. 
#include <boost/shared_ptr.hpp> // for mutex. 

namespace lsst {
namespace qserv {
namespace master {
// Forward
class SqlConfig;
class SqlConnection;

struct TableMergerError {
public:
    enum {NONE, IMPORT, MYSQLOPEN, MERGEWRITE, TERMINATE, 
	  MYSQLCONNECT, MYSQLEXEC} status;
    int errorCode;
    std::string description;
};

class TableMergerConfig {
public:
    TableMergerConfig(std::string targetDb_, std::string targetTable_,
		      std::string fixupSelect_,
		      std::string fixupPost_,
		      std::string user_, std::string socket_,
		      std::string mySqlCmd_) 
	:  targetDb(targetDb_),  targetTable(targetTable_),
	   fixupSelect(fixupSelect_), fixupPost(fixupPost_),
	   user(user_),  socket(socket_), mySqlCmd(mySqlCmd_)  
    {}

    std::string targetDb; // for final result, and imported result
    std::string targetTable;
    std::string fixupSelect;
    std::string fixupPost;
    std::string user;
    std::string socket;
    std::string mySqlCmd;
};


class TableMerger {
public:
    TableMerger(TableMergerConfig const& c);

    bool merge(std::string const& dumpFile, std::string const& tableName);
    
    TableMergerError const& getError() const { return _error; }
    std::string getTargetTable() const {return _config.targetTable; }

    bool finalize();
private:
    bool _applySql(std::string const& sql);
    bool _applySqlLocal(std::string const& sql);
    std::string _buildMergeSql(std::string const& tableName, bool create);
    void _fixupTargetName();
    bool _importResult(std::string const& dumpFile);

    static std::string const _dropSql;
    static std::string const _createSql;
    static std::string const _createFixSql;
    static std::string const _insertSql;
    static std::string const _cleanupSql;
    static std::string const _cmdBase;

    TableMergerConfig _config;
    std::string _loadCmd;
    boost::shared_ptr<SqlConfig> _sqlConfig;
    boost::shared_ptr<SqlConnection> _sqlConn;

    std::string _mergeTable;
    TableMergerError _error;
    long long _resultLimit;
    int _tableCount;
    boost::mutex _countMutex;
    boost::mutex _popenMutex;
    boost::mutex _sqlMutex;
    
};

}}} // namespace lsst::qserv::master
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_MASTER_TABLE_MERGER_H
