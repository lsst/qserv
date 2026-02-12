// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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

#include "ccontrol/ParseListener.h"

#include <sstream>
#include <string>
#include <vector>

// these must be included before Log.h because they have a function called LOGS
// that conflicts with the LOGS macro defined in Log.h
#include "antlr4-runtime.h"
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"
#include "parser/QSMySqlParserListener.h"

#include "lsst/log/Log.h"

#include "ccontrol/ParseAdapters.h"
#include "ccontrol/ParseHelpers.h"
#include "ccontrol/ParseAdaptersCBH.h"
#include "ccontrol/UserQuery.h"
#include "parser/ParseException.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.ParseListener");

}  // end namespace

// This macro creates the enterXXX and exitXXX function definitions, for functions declared in
// ParseListener.h; the enter function pushes the adapter onto the stack (with parent from top of the
// stack), and the exit function pops the adapter from the top of the stack.
#define ENTER_EXIT_PARENT(NAME)                                                        \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {               \
        pushAdapterStack<NAME##CBH, NAME##Adapter, QSMySqlParser::NAME##Context>(ctx); \
    }                                                                                  \
                                                                                       \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) { popAdapterStack<NAME##Adapter>(ctx); }

// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT to satisfy
// the QSMySqlParserListener class API but expects that the grammar element will not be used. The enter
// function throws an adapter_order_error so that if the grammar element is unexpectedly entered the query
// parsing will abort.
#define UNHANDLED(NAME)                                                                                      \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {                                     \
        throw parser::adapter_order_error("qserv can not parse query, near \"" + getQueryString(ctx) + '"'); \
    }                                                                                                        \
                                                                                                             \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}

// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT but does not
// push (or pop) an adapter on the stack. Other adapters are expected to handle the grammar element as may be
// appropraite.
#define IGNORED(NAME)                                                     \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {} \
                                                                          \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}

// This macro is similar to IGNORED, but allows the enter message to log a specific warning message when it is
// called.
#define IGNORED_WARN(NAME, WARNING)                                       \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {} \
                                                                          \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}

// assert that condition is true, otherwise log a message & throw an adapter_execution_error with the
// text of the query string that the context represents.
//
// CONDITION: boolean statement
//      The condition that is being asserted. True passes, false logs and throws.
// MESSAGE_STRING: string
//      A message for the log, it is not included in the exception.
// CTX: an antlr4::ParserRuleContext* (or derived class)
//      The antlr4 context that is used to get the segment of the query that is currently being
//      processed.
#define ASSERT_EXECUTION_CONDITION(CONDITION, MESSAGE_STRING, CTX)                                 \
    if (not(CONDITION)) {                                                                          \
        auto queryString = getQueryString(CTX);                                                    \
        throw parser::adapter_execution_error("Error parsing query, near \"" + queryString + '"'); \
    }

namespace lsst::qserv::ccontrol {

ParseListener::VecPairStr ParseListener::getTokenPairs(antlr4::CommonTokenStream& tokens,
                                                       QSMySqlLexer const& lexer) {
    VecPairStr ret;
    for (auto const& t : tokens.getTokens()) {
        std::string name(lexer.getVocabulary().getSymbolicName(t->getType()));
        if (name.empty()) {
            name = lexer.getVocabulary().getLiteralName(t->getType());
        }
        ret.push_back(make_pair(std::move(name), t->getText()));
    }
    return ret;
}

ParseListener::ParseListener(std::string const& statement,
                             shared_ptr<ccontrol::UserQueryResources> const& queryResources)
        : _statement(statement), _queryResources(queryResources) {}

shared_ptr<query::SelectStmt> ParseListener::getSelectStatement() const {
    return _rootAdapter->getSelectStatement();
}

shared_ptr<ccontrol::UserQuery> ParseListener::getUserQuery() const { return _rootAdapter->getUserQuery(); }

// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template <typename ParentCBH, typename ChildAdapter, typename Context>
shared_ptr<ChildAdapter> ParseListener::pushAdapterStack(Context* ctx) {
    auto p = dynamic_pointer_cast<ParentCBH>(_adapterStack.back());
    ASSERT_EXECUTION_CONDITION(
            p != nullptr,
            "can't acquire expected Adapter `" + getTypeName<ParentCBH>() + "` from top of listenerStack.",
            ctx);
    auto childAdapter = make_shared<ChildAdapter>(p, ctx, this);
    childAdapter->checkContext();
    _adapterStack.push_back(childAdapter);
    childAdapter->onEnter();
    return childAdapter;
}

template <typename ChildAdapter>
void ParseListener::popAdapterStack(antlr4::ParserRuleContext* ctx) {
    shared_ptr<Adapter> adapterPtr = _adapterStack.back();
    adapterPtr->onExit();
    _adapterStack.pop_back();
    // capturing adapterPtr and casting it to the expected type is useful as a sanity check that the enter &
    // exit functions are called in the correct order (balanced). The dynamic cast is of course not free and
    // this code could be optionally disabled or removed entirely if the check is found to be unnecesary or
    // adds too much of a performance penalty.
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(adapterPtr);
    ASSERT_EXECUTION_CONDITION(derivedPtr != nullptr,
                               "Top of listenerStack was not of expected type. "
                               "Expected: " +
                                       getTypeName<ChildAdapter>() + ", Actual: " + getTypeName(adapterPtr) +
                                       ", Are there out of order or unhandled listener exits?",
                               ctx);
}

string ParseListener::adapterStackToString() const {
    string ret;
    for (auto&& adapter : _adapterStack) {
        ret += adapter->name() + ", ";
    }
    return ret;
}

// ParseListener class methods

void ParseListener::enterRoot(QSMySqlParser::RootContext* ctx) {
    ASSERT_EXECUTION_CONDITION(_adapterStack.empty(), "RootAdatper should be the first entry on the stack.",
                               ctx);
    _rootAdapter = make_shared<RootAdapter>();
    _adapterStack.push_back(_rootAdapter);
    _rootAdapter->onEnter(ctx, this);
}

void ParseListener::exitRoot(QSMySqlParser::RootContext* ctx) { popAdapterStack<RootAdapter>(ctx); }

string ParseListener::getStringTree() const {
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    QSMySqlLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    QSMySqlParser parser(&tokens);
    tree::ParseTree* tree = parser.root();
    return tree->toStringTree(&parser);
}

string ParseListener::getTokens() const {
    using namespace antlr4;
    ANTLRInputStream input(_statement);
    QSMySqlLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    std::ostringstream t;
    t << util::printable(getTokenPairs(tokens, lexer));
    return t.str();
}

string ParseListener::getStatementString() const { return _statement; }

IGNORED(SqlStatements)
IGNORED(SqlStatement)
IGNORED(EmptyStatement)
ENTER_EXIT_PARENT(DmlStatement)
ENTER_EXIT_PARENT(SimpleSelect)
ENTER_EXIT_PARENT(QuerySpecification)
ENTER_EXIT_PARENT(SelectElements)
ENTER_EXIT_PARENT(SelectColumnElement)
ENTER_EXIT_PARENT(FromClause)
ENTER_EXIT_PARENT(TableSources)
ENTER_EXIT_PARENT(TableSourceBase)
ENTER_EXIT_PARENT(AtomTableItem)
ENTER_EXIT_PARENT(TableName)
ENTER_EXIT_PARENT(FullColumnName)
ENTER_EXIT_PARENT(FullId)
ENTER_EXIT_PARENT(Uid)
IGNORED(DecimalLiteral)
IGNORED(StringLiteral)
ENTER_EXIT_PARENT(PredicateExpression)
ENTER_EXIT_PARENT(ExpressionAtomPredicate)
ENTER_EXIT_PARENT(QservFunctionSpec)
ENTER_EXIT_PARENT(BinaryComparasionPredicate)
ENTER_EXIT_PARENT(ConstantExpressionAtom)
ENTER_EXIT_PARENT(FullColumnNameExpressionAtom)
ENTER_EXIT_PARENT(ComparisonOperator)
ENTER_EXIT_PARENT(AdministrationStatement)
ENTER_EXIT_PARENT(CallStatement)
ENTER_EXIT_PARENT(OrderByClause)
ENTER_EXIT_PARENT(OrderByExpression)
ENTER_EXIT_PARENT(InnerJoin)
ENTER_EXIT_PARENT(NaturalJoin)
ENTER_EXIT_PARENT(SelectSpec)
ENTER_EXIT_PARENT(SelectStarElement)
ENTER_EXIT_PARENT(SelectFunctionElement)
ENTER_EXIT_PARENT(SelectExpressionElement)
ENTER_EXIT_PARENT(GroupByItem)
ENTER_EXIT_PARENT(LimitClause)
ENTER_EXIT_PARENT(SetVariable)
ENTER_EXIT_PARENT(VariableClause)
ENTER_EXIT_PARENT(SimpleId)
ENTER_EXIT_PARENT(DottedId)
ENTER_EXIT_PARENT(NullNotnull)
ENTER_EXIT_PARENT(Constant)
ENTER_EXIT_PARENT(UidList)
ENTER_EXIT_PARENT(Expressions)
ENTER_EXIT_PARENT(Constants)
ENTER_EXIT_PARENT(AggregateFunctionCall)
ENTER_EXIT_PARENT(ScalarFunctionCall)
ENTER_EXIT_PARENT(UdfFunctionCall)
ENTER_EXIT_PARENT(AggregateWindowedFunction)
ENTER_EXIT_PARENT(ScalarFunctionName)
ENTER_EXIT_PARENT(FunctionArgs)
ENTER_EXIT_PARENT(FunctionArg)
ENTER_EXIT_PARENT(NotExpression)
IGNORED(QservFunctionSpecExpression)
ENTER_EXIT_PARENT(LogicalExpression)
ENTER_EXIT_PARENT(InPredicate)
ENTER_EXIT_PARENT(BetweenPredicate)
ENTER_EXIT_PARENT(IsNullPredicate)
ENTER_EXIT_PARENT(LikePredicate)
ENTER_EXIT_PARENT(NestedExpressionAtom)
ENTER_EXIT_PARENT(MathExpressionAtom)
ENTER_EXIT_PARENT(FunctionCallExpressionAtom)
ENTER_EXIT_PARENT(BitExpressionAtom)
ENTER_EXIT_PARENT(LogicalOperator)
ENTER_EXIT_PARENT(BitOperator)
ENTER_EXIT_PARENT(MathOperator)
IGNORED_WARN(KeywordsCanBeId, "Keyword reused as ID")  // todo emit a warning?
ENTER_EXIT_PARENT(FunctionNameBase)

}  // namespace lsst::qserv::ccontrol
