// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
/**
  * @file
  *
  * @brief Implementation of a SelectParser
  *
  *  SelectParser is the top-level manager for everything attached to
  *  parsing the top-level SQL query. Given an input query and a
  *  configuration, computes a query info structure, name ref list,
  *  and a "query plan".
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/SelectParser.h"

// System headers
#include <cstdio>
#include <functional>
#include <memory>
#include <strings.h>
#include <vector>

// Third-party headers
#include "boost/bind.hpp"

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "global/Bug.h"
#include "parser/ParseException.h"
#include "query/SelectStmt.h"
#include "util/IterableFormatter.h"

// these must be included before Log.h because they have a function called LOGS
// that conflicts with the LOGS macro defined in Log.h
#include "antlr4-runtime.h"
#include "ccontrol/QSMySqlListener.h"
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"
#include "parser/QSMySqlParserListener.h"

// must come after QSMySqlLexer & Parser because of namespace collision
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.SelectParser");

// For the current query, this returns a list where each pair contains a bit of the string from the query
// and how antlr4 tokenized that bit of string. It is useful for debugging problems where antlr4 did not
// parse a query as expected, in the case where the string was not tokenized as expected.
typedef std::vector<std::pair<std::string, std::string>> VecPairStr;
VecPairStr getTokenPairs(antlr4::CommonTokenStream & tokens, QSMySqlLexer & lexer) {
    VecPairStr ret;
    for (auto&& t : tokens.getTokens()) {
        std::string name = lexer.getVocabulary().getSymbolicName(t->getType());
        if (name.empty()) {
            name = lexer.getVocabulary().getLiteralName(t->getType());
        }
        ret.push_back(make_pair(std::move(name), t->getText()));
    }
    return ret;
}

}


namespace lsst {
namespace qserv {
namespace parser {


////////////////////////////////////////////////////////////////////////
// AntlrParser -- Antlr parsing complex
////////////////////////////////////////////////////////////////////////
std::string AntlrParser::stateString(State s) {
    switch (s) {
        default:
            return "??";

        case INIT:
            return "INIT";

        case SETUP_DONE:
            return "SETUP_DONE";

        case RUN_DONE:
            return "RUN_DONE";
    }
}


void AntlrParser::changeState(State to) {
    switch (_state) {
        default:
            throw ParseException("Parse error(INTERNAL): unhandled state transition value: "
                    + std::to_string(_state));

        case INIT:
            if (SETUP_DONE != to) {
                throw ParseException("Parse error(INTERNAL):invalid state transition from INIT to "
                        + stateString(to));
            }
            _state = SETUP_DONE;
            break;

        case SETUP_DONE:
            if (RUN_DONE != to) {
                throw ParseException("Parse error(INTERNAL):invalid state transition from SETUP_DONE to "
                        + stateString(to));
            }
            _state = RUN_DONE;
            break;

        case RUN_DONE:
            // there are no valid transitions from RUN_DONE
            throw ParseException("Parse error(INTERNAL):invalid state transition from RUN_DONE to "
                    + stateString(to));
    }
}


class Antlr4ErrorStrategy : public antlr4::DefaultErrorStrategy {
public:
    explicit Antlr4ErrorStrategy(std::string const& statement) : _statement(statement) {}
    Antlr4ErrorStrategy() = delete;
    Antlr4ErrorStrategy(Antlr4ErrorStrategy const&) = delete;
    Antlr4ErrorStrategy& operator=(Antlr4ErrorStrategy const&) = delete;

private:
    void recover(antlr4::Parser *recognizer, std::exception_ptr e) override {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ <<
                " antlr4 could not make a parse tree out of the input statement:" << _statement);
        throw ParseException(std::string("Failed to instantiate query: \"") + _statement +
                std::string("\""));
    }

    antlr4::Token* recoverInline(antlr4::Parser *recognizer) override {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ <<
                " antlr4 could not make a parse tree out of the input statement:" << _statement);
        throw ParseException(std::string("Failed to instantiate query: \"") + _statement +
                std::string("\""));
    }

    void sync(antlr4::Parser *recognizer) override {
        // we want this function to be a no-op so we override it.
    }

    std::string const& _statement;
};


class NonRecoveringQSMySqlLexer : public QSMySqlLexer {
public:
    explicit NonRecoveringQSMySqlLexer(antlr4::CharStream *input, std::string const & statement)
        : QSMySqlLexer(input), _statement(statement) {}
    NonRecoveringQSMySqlLexer() = delete;
    NonRecoveringQSMySqlLexer(NonRecoveringQSMySqlLexer const&) = delete;
    NonRecoveringQSMySqlLexer& operator=(NonRecoveringQSMySqlLexer const&) = delete;


private:
    virtual void recover(const antlr4::LexerNoViableAltException &e) {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ <<
                "antlr4 could not tokenize the input statement:" << _statement);
        throw ParseException(std::string("Failed to instantiate query: \"") + _statement +
                std::string("\""));
    }

    std::string const& _statement;
};



std::shared_ptr<Antlr4Parser> Antlr4Parser::create(std::string const & q,
                                                   std::shared_ptr<ccontrol::UserQueryResources> queryResources) {
    return std::shared_ptr<Antlr4Parser>(new Antlr4Parser(q, queryResources));
}


void Antlr4Parser::setup() {
    changeState(SETUP_DONE);
    _listener = std::make_shared<ccontrol::QSMySqlListener>(
            std::static_pointer_cast<ccontrol::ListenerDebugHelper>(shared_from_this()),
            _queryResources);
}


void Antlr4Parser::run() {
    changeState(RUN_DONE);
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    NonRecoveringQSMySqlLexer lexer(&input, _statement);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    LOGS(_log, LOG_LVL_TRACE, "Parsed tokens:" << util::printable(getTokenPairs(tokens, lexer)));
    QSMySqlParser parser(&tokens);
    parser.setErrorHandler(std::make_shared<Antlr4ErrorStrategy>(_statement));
    tree::ParseTree *tree = parser.root();
    tree::ParseTreeWalker walker;
    antlr4::tree::ParseTreeListener* listener = _listener.get();
    walker.walk(listener, tree);
}


query::SelectStmt::Ptr Antlr4Parser::getStatement() {
    return _listener->getSelectStatement();
}


std::shared_ptr<ccontrol::UserQuery> Antlr4Parser::getUserQuery() {
    return _listener->getUserQuery();
}


// TODO these funcs can probably be written with less code duplication
// and also without stringstream
std::string Antlr4Parser::getStringTree() const {
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    QSMySqlLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    QSMySqlParser parser(&tokens);
    tree::ParseTree *tree = parser.root();
    return tree->toStringTree(&parser);
}


std::string Antlr4Parser::getTokens() const {
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    QSMySqlLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    std::ostringstream t;
    t << util::printable(getTokenPairs(tokens, lexer));
    return t.str();
}


std::string Antlr4Parser::getStatementString() const {
    return _statement;
}


Antlr4Parser::Antlr4Parser(std::string const& q,
                           std::shared_ptr<ccontrol::UserQueryResources> const& queryResources)
    : _statement(q), _queryResources(queryResources)
{}


////////////////////////////////////////////////////////////////////////
// class SelectParser
////////////////////////////////////////////////////////////////////////


SelectParser::Ptr SelectParser::newInstance(std::string const& statement) {
    return std::shared_ptr<SelectParser>(new SelectParser(statement));
}


std::shared_ptr<query::SelectStmt> SelectParser::makeSelectStmt(std::string const& statement) {
    auto parser = newInstance(statement);
    return parser->getSelectStmt();
}


SelectParser::SelectParser(std::string const& statement)
    :_statement(statement) {
    _aParser = Antlr4Parser::create(_statement, nullptr);
    _aParser->setup();
    _aParser->run();
}


std::shared_ptr<query::SelectStmt> SelectParser::getSelectStmt() {
    return _aParser->getStatement();
}


}}} // namespace lsst::qserv::parser
