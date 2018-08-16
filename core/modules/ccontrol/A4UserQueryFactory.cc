// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 AURA/LSST.
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

#include "ccontrol/A4UserQueryFactory.h"

#include <string>
#include <utility>

#include "antlr4-runtime.h"

// these must be included before Log.h because they have a function called LOGS
// that conflicts with the LOGS macro defined in Log.h
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"

#include "lsst/log/Log.h"

#include "parser/QSMySqlListener.h"
#include "query/SelectStmt.h"
#include "util/IterableFormatter.h"


// antlr4 C++ runtime seems to require that we use namespace antlr4; trying to use it with classes
// results in at least one error where the `tree` decl assumes that it's already operating in the
// antlr4 namespace. I may be wrong about this though; it needs research (before merge to master).
// If I'm right we should see about fixing the antlr4 cpp runtime & issuing a PR to them.
using namespace antlr4;
using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.A4UserQueryFactory");

// For the current query, this returns a list where each pair contains a bit of the string from the query
// and how antlr4 tokenized that bit of string. It is useful for debugging problems where antlr4 did not
// parse a query as expected, in the case where the string was not tokenized as expected.
vector<pair<string, string>> getTokenPairs(CommonTokenStream & tokens, QSMySqlLexer & lexer) {
    vector<pair<string, string>> ret;
    for (auto&& t : tokens.getTokens()) {
        string name = lexer.getVocabulary().getSymbolicName(t->getType());
        if (name.empty()) {
            name = lexer.getVocabulary().getLiteralName(t->getType());
        }
        ret.push_back(make_pair(name, t->getText()));
    }
    return ret;
}

}

namespace lsst {
namespace qserv {
namespace ccontrol {

shared_ptr<query::SelectStmt> a4NewUserQuery(const string& userQuery) {
    ANTLRInputStream input(userQuery);
    QSMySqlLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    LOGS(_log, LOG_LVL_TRACE, "New user query, antlr4 tokens: " <<  util::printable(getTokenPairs(tokens, lexer)));
    QSMySqlParser parser(&tokens);
    tree::ParseTree *tree = parser.root();
    LOGS(_log, LOG_LVL_TRACE, "New user query, antlr4 string tree: " << tree->toStringTree(&parser));
    tree::ParseTreeWalker walker;
    parser::QSMySqlListener listener;
    walker.walk(&listener, tree);
    return listener.getSelectStatement();
}


}}}
