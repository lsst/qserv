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

#include "antlr4-runtime.h"

// these must be included before Log.h because they have a function called LOGS
// that conflicts with the LOGS macro defined in Log.h
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"

#include "lsst/log/Log.h"

#include "parser/QSMySqlListener.h"
#include "query/SelectStmt.h"



// antlr4 C++ runtime seems to require that we use namespace antlr4; trying to use it with classes
// results in at least one error where the `tree` decl assumes that it's already operating in the
// antlr4 namespace. I may be wrong about this though; it needs research (before merge to master).
// If I'm right we should see about fixing the antlr4 cpp runtime & issuing a PR to them.
using namespace antlr4;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.A4UserQueryFactory");
}

namespace lsst {
namespace qserv {
namespace ccontrol {


std::shared_ptr<query::SelectStmt> a4NewUserQuery(const std::string& userQuery) {
    try {
        ANTLRInputStream input(userQuery);
        QSMySqlLexer lexer(&input);
        CommonTokenStream tokens(&lexer);
        tokens.fill();
        QSMySqlParser parser(&tokens);
        tree::ParseTree *tree = parser.root();
        LOGS(_log, LOG_LVL_DEBUG, "New user query, antlr4 string tree: " << tree->toStringTree(&parser));
        tree::ParseTreeWalker walker;
        parser::QSMySqlListener listener;
        walker.walk(&listener, tree);
        return listener.getSelectStatement();
    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, "Antlr4 error: " << e.what());
        return nullptr;
    }
}


}}}
