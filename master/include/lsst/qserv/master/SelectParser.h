// -*- LSST-C++ -*-
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
// SelectParser is the top-level manager for everything attached to
// parsing a top-level SQL select statement. While it is the spiritual
// successor to the original SqlParseRunner, it attempts to modularize
// and delegate responsibility for improved readability,
// maintainability, and extensibility.
 
#ifndef LSST_QSERV_MASTER_SELECTPARSER_H
#define LSST_QSERV_MASTER_SELECTPARSER_H

// C++ standard
#include <list>
#include <sstream>

#include <boost/shared_ptr.hpp>
// package
#include "lsst/qserv/master/common.h"
# if 0
#include "lsst/qserv/master/mergeTypes.h"
#include "lsst/qserv/master/AggregateMgr.h"
#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/Templater.h"
#include "lsst/qserv/master/parseHandlers.h"

// Forward
class ASTFactory;
class SqlSQL2Lexer;
class SqlSQL2Parser;
#endif
namespace lsst {
namespace qserv {
namespace master {

// Forward
class AntlrParser; // Internally-defined in SelectParser.cc

#if 0
class LimitHandler; // Parse handler
class OrderByHandler; // Parse handler
class SpatialUdfHandler; // 
class TableRefChecker; // alias/ref checker.
class TableNamer; // alias/ref namespace mgr
#endif
class SelectStmt;

/// class SelectParser - drives the ANTLR-generated SQL parser for a
/// SELECT statement. Attaches some simple handlers that populate a
/// corresponding data structure, which can then be processed and
/// evaluated to determine query generation and dispatch.
class SelectParser {
public:
    typedef boost::shared_ptr<SelectParser> Ptr;

    // From SqlParseRunner (interface)
    static Ptr newInstance(std::string const& statement);

    // (Deprecated) Pass in a list of allowed names
    void setup();
    // @return Original select statement
    std::string const& getStatement() const { return _statement; }
    
    boost::shared_ptr<SelectStmt> getSelectStmt() { return _selectStmt; }
    // Move to QueryPlan
#if 0
    // Better as: "chunkquery" and "post query fix"
    std::string getParseResult();
    std::string getAggParseResult();

    // These are properties retrievable after the parse, in the query plan
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
#endif
private:
    // Setup and construction
    SelectParser(std::string const& statement);
    // Init
    //void _readConfig(StringMap const& m);
    // Post-parse

    std::string const _statement;
    boost::shared_ptr<SelectStmt> _selectStmt;    
    boost::shared_ptr<AntlrParser> _aParser;

    
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_SELECTPARSER_H
