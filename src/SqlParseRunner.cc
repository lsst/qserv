// Boost
#include <boost/make_shared.hpp>

// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2Lexer.hpp"

#include "lsst/qserv/master/SqlParseRunner.h"
#include "lsst/qserv/master/Substitution.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;
using std::stringstream;

// Helper
class LimitHandler : public VoidOneRefFunc {
public: 
    LimitHandler(qMaster::SqlParseRunner& spr) : _spr(spr) {}
    virtual ~LimitHandler() {}
    virtual void operator()(antlr::RefAST i) {
        std::stringstream ss(i->getText());
        int limit;
        ss >> limit;
        _spr.setLimit(limit);
        std::cout << "Got limit -> " << limit << std::endl;            
    }
private:
    qMaster::SqlParseRunner& _spr;
};

boost::shared_ptr<qMaster::SqlParseRunner> 
qMaster::newSqlParseRunner( std::string const& statement, 
                            std::string const& delimiter,
                            std::string const& defaultDb) {
    return boost::make_shared<qMaster::SqlParseRunner>(statement, 
                                                       delimiter, 
                                                       defaultDb);
}

qMaster::SqlParseRunner::SqlParseRunner(std::string const& statement, 
                                        std::string const& delimiter,
                                        std::string const& defaultDb) :
    _statement(statement),
    _stream(statement, stringstream::in | stringstream::out),
    _lexer(new SqlSQL2Lexer(_stream)),
    _parser(new SqlSQL2Parser(*_lexer)),
    _delimiter(delimiter),
    _factory(new ASTFactory()),
    _templater(_delimiter, _factory.get(), defaultDb)
{ 
    std::cout << "(int)PARSING:"<< statement << std::endl;
}

void qMaster::SqlParseRunner::setup(std::list<std::string> const& names) {
    _templater.setKeynames(names.begin(), names.end());
    _parser->_columnRefHandler = _templater.newColumnHandler();
    _parser->_qualifiedNameHandler = _templater.newTableHandler();
    _tableListHandler = _templater.newTableListHandler();
    _parser->_tableListHandler = _tableListHandler;
    _parser->_setFctSpecHandler = _aggMgr.getSetFuncHandler();
    _parser->_aliasHandler = _aggMgr.getAliasHandler();
    _parser->_selectListHandler = _aggMgr.getSelectListHandler();
    _parser->_selectStarHandler = _aggMgr.newSelectStarHandler();
    _parser->_groupByHandler = _aggMgr.getGroupByHandler();
    _parser->_groupColumnHandler = _aggMgr.getGroupColumnHandler();
    _parser->_limitHandler.reset(new LimitHandler(*this));
}

std::string qMaster::SqlParseRunner::getParseResult() {
    if(_errorMsg.empty() && _parseResult.empty()) {
        _computeParseResult();
    }
    return _parseResult;
}
std::string qMaster::SqlParseRunner::getAggParseResult() {
    if(_errorMsg.empty() && _aggParseResult.empty()) {
        _computeParseResult();
    }
    return _aggParseResult;
}
void qMaster::SqlParseRunner::_computeParseResult() {
    bool hasBadDbs = false;
    try {
        _parser->initializeASTFactory(*_factory);
        _parser->setASTFactory(_factory.get());
        _parser->sql_stmt();
        _aggMgr.postprocess();
        hasBadDbs = 0 < _templater.getBadDbs().size();
        RefAST ast = _parser->getAST();
        if (ast) {
            //std::cout << "fixupSelect " << getFixupSelect();
            //std::cout << "passSelect " << getPassSelect();
            // ";" is not in the AST, so add it back.
            _parseResult = walkTreeString(ast);
            _aggMgr.applyAggPass();
            _aggParseResult = walkTreeString(ast);
            if(_tableListHandler->getHasSubChunks()) {
                _makeOverlapMap();
                _aggParseResult = _composeOverlap(_aggParseResult);
                _parseResult = _composeOverlap(_parseResult);
            }
            _aggParseResult += ";";
            _parseResult += ";";
            _mFixup.select = _aggMgr.getFixupSelect();
            _mFixup.post = _aggMgr.getFixupPost();
            //"", /* FIXME need orderby */
            _mFixup.needsFixup = _aggMgr.getHasAggregate() 
                || (_mFixup.limit != -1) || (!_mFixup.orderBy.empty());
        } else {
            _errorMsg = "Error: no AST from parse";
        }
    } catch( antlr::ANTLRException& e ) {
        _errorMsg =  "Parse exception: " + e.toString();
    } catch( std::exception& e ) {
        _errorMsg = std::string("General exception: ") + e.what();
    }
    if(hasBadDbs) {
        _errorMsg += " Query references prohibited dbs.";
    }
    return; 
}

void qMaster::SqlParseRunner::_makeOverlapMap() {
    qMaster::Templater::IntMap im = _tableListHandler->getUsageCount();
    qMaster::Templater::IntMap::iterator e = im.end();
    for(qMaster::Templater::IntMap::iterator i = im.begin(); i != e; ++i) {
        _overlapMap[i->first+"_sc2"] = i->first + "_sfo";
    }

}

std::string qMaster::SqlParseRunner::_composeOverlap(std::string const& query) {
    Substitution s(query, _delimiter, false);
    return query + " union " + s.transform(_overlapMap);
}

bool qMaster::SqlParseRunner::getHasAggregate() {
    if(_errorMsg.empty() && _parseResult.empty()) {
        _computeParseResult();
    }
    return _aggMgr.getHasAggregate();
}

