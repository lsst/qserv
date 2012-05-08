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
 
#ifndef LSST_QSERV_MASTER_SQLPARSERUNNER_H
#define LSST_QSERV_MASTER_SQLPARSERUNNER_H

// C++ standard
#include <list>
#include <sstream>

// package
#include "lsst/qserv/master/mergeTypes.h"
#include "lsst/qserv/master/AggregateMgr.h"
#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/Templater.h"
#include "lsst/qserv/master/parseHandlers.h"

// Forward
class ASTFactory;
class SqlSQL2Lexer;
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {

// Forward
class LimitHandler; // Parse handler
class OrderByHandler; // Parse handler
class SpatialUdfHandler; // 
class TableRefChecker; // alias/ref checker.
class TableNamer; // alias/ref namespace mgr

/// class SqlParseRunner - drives the ANTLR-generated SQL parser.
/// Attaches a set of handlers to the grammar and triggers the parsing
/// of a string as a SQL select statement, then provides a templated
/// SQL statement that can undergo substitution to generate subqueries.
class SqlParseRunner {
public:
    typedef boost::shared_ptr<SqlParseRunner> Ptr;
    typedef Templater::IntMap IntMap;

    static Ptr newInstance(std::string const& statement, 
                           std::string const& delimiter,
                           StringMap const& config);
    void setup(std::list<std::string> const& names);
    std::string const& getStatement() const { return _statement; }
    std::string getParseResult();
    std::string getAggParseResult();
    bool getHasChunks() const;
    bool getHasSubChunks() const;

    std::string getFixupSelect() {
	return _aggMgr.getFixupSelect();
    }
    std::string getFixupPost() {
	return _aggMgr.getFixupPost();
    }
    MergeFixup const& getMergeFixup() const {
        return _mFixup;
    }
    std::string getPassSelect() {
	return _aggMgr.getPassSelect();
    }
    bool getHasAggregate();
    std::string const& getError() const {
	return _errorMsg;
    }
    void addMungedSpatial(std::string const& mungedTable,
                          std::string const& refTable);
    void updateTableConfig(std::string const& tName, StringMap const& m);
    void addHintExpr(std::vector<std::string> const& vec);

private:
    class SpatialTableNotifier;
    class FromHandler;
    friend class FromHandler;

    // Setup and construction
    SqlParseRunner(std::string const& statement, 
                   std::string const& delimiter,
                   StringMap const& config);
    void _readConfig(StringMap const& m);

    // Helpers for operation
    void _computeParseResult();
    void _makeOverlapMap();
    std::string _interpretBadDbs(StringList const& bd);

    // Parse handlers
    friend class LimitHandler;
    friend class OrderByHandler;
    void _setLimitForHandler(int limit) { 
        _mFixup.limit = limit; 
    }
    void _setOrderByForHandler(std::string const& cols) { 
        _mFixup.orderBy = cols; 
    }
    
    // Table partition handling
    class PartitionTupleProcessor;
    friend class ParititonTupleProcessor;
    // Query hint plumbing
    class HintTupleProcessor;

    std::string _statement;
    std::stringstream _stream;
    boost::shared_ptr<ASTFactory> _factory;
    boost::shared_ptr<SqlSQL2Lexer> _lexer;
    boost::shared_ptr<SqlSQL2Parser> _parser;
    std::string _delimiter;

    std::map<std::string, StringMap> _tableConfigMap;

    StringMap _mungeMap;
    Templater _templater;
    AliasMgr _aliasMgr;
    AggregateMgr _aggMgr;
    boost::shared_ptr<Templater::TableListHandler>  _tableListHandler;
    boost::shared_ptr<TableRefChecker> _refChecker;
    boost::shared_ptr<TableNamer> _tableNamer;
    boost::shared_ptr<SpatialUdfHandler> _spatialUdfHandler;
    
    std::string _parseResult;
    std::string _aggParseResult;
    std::string _errorMsg;
    StringMap _overlapMap;
    MergeFixup _mFixup;

    std::map<std::string, int> _dbWhiteList;

};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_SQLPARSERUNNER_H
