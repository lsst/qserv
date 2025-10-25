// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 LSST.
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

// Class header
#include "ccontrol/ParseRunner.h"

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "parser/ParseException.h"
#include "query/SelectStmt.h"
#include "util/IterableFormatter.h"

// these must be included before Log.h because they have a function called LOGS
// that conflicts with the LOGS macro defined in Log.h
#include "antlr4-runtime.h"
#include "ccontrol/ParseListener.h"
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"

// must come after QSMySqlLexer & Parser because of namespace collision
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.ParseRunner");

class Antlr4ErrorStrategy : public antlr4::DefaultErrorStrategy {
public:
    explicit Antlr4ErrorStrategy(std::string const& statement) : _statement(statement) {}
    Antlr4ErrorStrategy() = delete;
    Antlr4ErrorStrategy(Antlr4ErrorStrategy const&) = delete;
    Antlr4ErrorStrategy& operator=(Antlr4ErrorStrategy const&) = delete;

private:
    void recover(antlr4::Parser* recognizer, std::exception_ptr e) override {
        LOGS(_log, LOG_LVL_ERROR,
             __FUNCTION__ << " antlr4 could not make a parse tree out of the input statement:" << _statement);
        throw lsst::qserv::parser::ParseException(std::string("Failed to instantiate query: \"") +
                                                  _statement + '"');
    }

    antlr4::Token* recoverInline(antlr4::Parser* recognizer) override {
        LOGS(_log, LOG_LVL_ERROR,
             __FUNCTION__ << " antlr4 could not make a parse tree out of the input statement:" << _statement);
        throw lsst::qserv::parser::ParseException(std::string("Failed to instantiate query: \"") +
                                                  _statement + '"');
    }

    void sync(antlr4::Parser* recognizer) override {
        // we want this function to be a no-op so we override it.
        // LOGS(_log, LOG_LVL_TRACE, "run:*");
    }

    std::string const& _statement;
};

class NonRecoveringQSMySqlLexer : public QSMySqlLexer {
public:
    explicit NonRecoveringQSMySqlLexer(antlr4::CharStream* input, std::string const& statement)
            : QSMySqlLexer(input), _statement(statement) {}
    NonRecoveringQSMySqlLexer() = delete;
    NonRecoveringQSMySqlLexer(NonRecoveringQSMySqlLexer const&) = delete;
    NonRecoveringQSMySqlLexer& operator=(NonRecoveringQSMySqlLexer const&) = delete;

private:
    virtual void recover(const antlr4::LexerNoViableAltException& e) {
        LOGS(_log, LOG_LVL_ERROR,
             __FUNCTION__ << "antlr4 could not tokenize the input statement:" << _statement);
        throw lsst::qserv::parser::ParseException(std::string("Failed to instantiate query: \"") +
                                                  _statement + '"');
    }

    std::string const& _statement;
};

}  // namespace

namespace lsst::qserv::ccontrol {

std::shared_ptr<query::SelectStmt> ParseRunner::makeSelectStmt(std::string const& statement) {
    auto parser = std::make_shared<ParseRunner>(statement);
    return parser->getSelectStmt();
}

ParseRunner::ParseRunner(std::string const& statement) : _statement(statement) { run(); }

ParseRunner::ParseRunner(std::string const& statement,
                         std::shared_ptr<UserQueryResources> const& queryResources)
        : _statement(statement), _queryResources(queryResources) {
    run();
}

void ParseRunner::run() {
    LOGS(_log, LOG_LVL_TRACE, "run:1");
    _listener = std::make_shared<ParseListener>(_statement, _queryResources);
    LOGS(_log, LOG_LVL_TRACE, "run:2");
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    LOGS(_log, LOG_LVL_TRACE, "run:3");
    NonRecoveringQSMySqlLexer lexer(&input, _statement);
    LOGS(_log, LOG_LVL_TRACE, "run:4");
    CommonTokenStream tokens(&lexer);
    LOGS(_log, LOG_LVL_TRACE, "run:5");
    tokens.fill();
    LOGS(_log, LOG_LVL_TRACE, "run:6");
    LOGS(_log, LOG_LVL_TRACE,
         "Parsed tokens:" << util::printable(ParseListener::getTokenPairs(tokens, lexer)));
    LOGS(_log, LOG_LVL_TRACE, "run:7");
    QSMySqlParser parser(&tokens);
    LOGS(_log, LOG_LVL_TRACE, "run:8");
    parser.setErrorHandler(std::make_shared<Antlr4ErrorStrategy>(_statement));
    LOGS(_log, LOG_LVL_TRACE, "run:9");
    tree::ParseTree* tree = parser.root();
    LOGS(_log, LOG_LVL_TRACE, "run:10");
    tree::ParseTreeWalker walker;
    LOGS(_log, LOG_LVL_TRACE, "run:11");
    antlr4::tree::ParseTreeListener* listener = _listener.get();
    LOGS(_log, LOG_LVL_TRACE, "run:12");
    walker.walk(listener, tree);
    LOGS(_log, LOG_LVL_TRACE, "run:13");
}

std::shared_ptr<query::SelectStmt> ParseRunner::getSelectStmt() { return _listener->getSelectStatement(); }

std::shared_ptr<UserQuery> ParseRunner::getUserQuery() { return _listener->getUserQuery(); }

}  // namespace lsst::qserv::ccontrol
