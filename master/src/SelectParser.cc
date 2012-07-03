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
// parsing the top-level SQL query. Given an input query and a
// configuration, computes a query info structure, name ref list, and
// a "query plan".  


// Standard
#include <functional>
#include <cstdio>
#include <strings.h>

// Boost
#include <boost/make_shared.hpp>
#include <boost/bind.hpp>

// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2Lexer.hpp"
#include "SqlSQL2TokenTypes.hpp"
#if 0
#include "lsst/qserv/master/Callback.h"
#include "lsst/qserv/master/SqlParseRunner.h"
#include "lsst/qserv/master/Substitution.h"
#include "lsst/qserv/master/stringUtil.h"
#include "lsst/qserv/master/TableRefChecker.h"
#include "lsst/qserv/master/TableNamer.h"
#include "lsst/qserv/master/TableRemapper.h"
#include "lsst/qserv/master/SpatialUdfHandler.h"
#endif
#include "lsst/qserv/master/SelectStmt.h"

#include "lsst/qserv/master/SelectParser.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/SelectFactory.h"

#include <antlr/CommonAST.hpp>
// namespace modifiers
namespace qMaster = lsst::qserv::master;

////////////////////////////////////////////////////////////////////////
// AntlrParser -- Antlr parsing complex
////////////////////////////////////////////////////////////////////////
class qMaster::AntlrParser {
public:
    AntlrParser(std::string const& q) 
        : statement(q), 
          stream(q, std::stringstream::in | std::stringstream::out),
          lexer(stream),
          parser(lexer)
        {}
    void run() {
        parser.initializeASTFactory(factory);
        parser.setASTFactory(&factory);
        parser.sql_stmt();
        explore();
    }
    void explore();
    std::string statement;
    std::stringstream stream;
    ASTFactory factory;
    SqlSQL2Lexer lexer;
    SqlSQL2Parser parser;
};
void 
qMaster::AntlrParser::explore() {
    RefAST a = parser.getAST();
//    std::cout << "wholething: " << walkIndentedString(a) << std::endl;
//    std::cout << "printing walktree \n";
//    printIndented(a);

}

////////////////////////////////////////////////////////////////////////
// class SelectParser
////////////////////////////////////////////////////////////////////////

// Static factory function
qMaster::SelectParser::Ptr 
qMaster::SelectParser::newInstance(std::string const& statement) {
    return boost::shared_ptr<SelectParser>(new SelectParser(statement));
}

// Construtor
qMaster::SelectParser::SelectParser(std::string const& statement)
    :_statement(statement) {
}

void 
qMaster::SelectParser::setup() {
    _selectStmt.reset(new SelectStmt());
    _aParser.reset(new AntlrParser(_statement));
    // model 3: parse tree construction to build intermediate expr.
    SelectFactory sf;
    sf.attachTo(_aParser->parser);
    _aParser->run();
    _selectStmt = sf.getStatement();
    _selectStmt->diagnose();
}
