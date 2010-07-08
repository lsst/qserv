#ifndef LSST_QSERV_MASTER_SQLPARSERUNNER_H
#define LSST_QSERV_MASTER_SQLPARSERUNNER_H

// C++ standard
#include <list>
#include <sstream>

// package
#include "lsst/qserv/master/parser.h"
#include "lsst/qserv/master/AggregateMgr.h"
#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/Templater.h"


// Forward
class ASTFactory;
class SqlSQL2Lexer;
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {

class SqlParseRunner {
/// SQL parser interaction manager

public:
    SqlParseRunner(std::string const& statement, 
                   std::string const& delimiter,
                   std::string const& defaultDb="");
    void setup(std::list<std::string> const& names);
    std::string getParseResult();
    std::string getAggParseResult();
    bool getHasChunks() const { 
	return _tableListHandler->getHasChunks();
    }
    bool getHasSubChunks() const { 
	return _tableListHandler->getHasSubChunks();
    }
    std::string getFixupSelect() {
	return _aggMgr.getFixupSelect();
    }
    std::string getFixupPost() {
	return _aggMgr.getFixupPost();
    }
    std::string getPassSelect() {
	return _aggMgr.getPassSelect();
    }
    bool getHasAggregate();
    std::string const& getError() const {
	return _errorMsg;
    }
    int getLimit() const {
        return _limit;
    }
    void setLimit(int lim) {
        _limit = lim;
    }

private:
    void _computeParseResult();
    void _makeOverlapMap();
    std::string _composeOverlap(std::string const& query);

    std::string _statement;
    std::stringstream _stream;
    boost::shared_ptr<ASTFactory> _factory;
    boost::shared_ptr<SqlSQL2Lexer> _lexer;
    boost::shared_ptr<SqlSQL2Parser> _parser;
    std::string _delimiter;
    Templater _templater;
    AggregateMgr _aggMgr;
    boost::shared_ptr<Templater::TableListHandler>  _tableListHandler;
    
    std::string _parseResult;
    std::string _aggParseResult;
    std::string _errorMsg;
    StringMapping _overlapMap;
    int _limit;
};

boost::shared_ptr<SqlParseRunner> newSqlParseRunner(std::string const& statement, 
                                                    std::string const& delimiter,
                                                    std::string const& defaultDb="");

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_SQLPARSERUNNER_H
