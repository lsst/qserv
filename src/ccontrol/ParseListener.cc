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
        LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " '" << getQueryString(ctx) << "'"); \
        pushAdapterStack<NAME##CBH, NAME##Adapter, QSMySqlParser::NAME##Context>(ctx); \
    }                                                                                  \
                                                                                       \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {                \
        LOGS(_log, LOG_LVL_TRACE, __FUNCTION__);                                       \
        popAdapterStack<NAME##Adapter>(ctx);                                           \
    }

// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT to satisfy
// the QSMySqlParserListener class API but expects that the grammar element will not be used. The enter
// function throws an adapter_order_error so that if the grammar element is unexpectedly entered the query
// parsing will abort.
#define UNHANDLED(NAME)                                                                                      \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {                                     \
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ << " is UNHANDLED for '" << getQueryString(ctx) << "'");      \
        throw parser::adapter_order_error("qserv can not parse query, near \"" + getQueryString(ctx) + '"'); \
    }                                                                                                        \
                                                                                                             \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}

// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT but does not
// push (or pop) an adapter on the stack. Other adapters are expected to handle the grammar element as may be
// appropraite.
#define IGNORED(NAME)                                                    \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
        LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED");        \
    }                                                                    \
                                                                         \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {  \
        LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED");        \
    }

// This macro is similar to IGNORED, but allows the enter message to log a specific warning message when it is
// called.
#define IGNORED_WARN(NAME, WARNING)                                                                  \
    void ParseListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) {                             \
        LOGS(_log, LOG_LVL_WARN,                                                                     \
             __FUNCTION__ << " is IGNORED, in '" << getQueryString(ctx) << "' warning:" << WARNING); \
    }                                                                                                \
                                                                                                     \
    void ParseListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {                              \
        LOGS(_log, LOG_LVL_TRACE,                                                                    \
             __FUNCTION__ << " is IGNORED, see warning in enter-function log entry, above.");        \
    }

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
#define ASSERT_EXECUTION_CONDITION(CONDITION, MESSAGE_STRING, CTX)                                           \
    if (not(CONDITION)) {                                                                                    \
        auto queryString = getQueryString(CTX);                                                              \
        LOGS(_log, LOG_LVL_ERROR,                                                                            \
             "Execution condition assertion failure:"                                                        \
                     << getTypeName(this) << "::" << __FUNCTION__ << " messsage:\"" << MESSAGE_STRING << '"' \
                     << ", in query:" << getStatementString() << ", in or around query segment: '"           \
                     << queryString << "'"                                                                   \
                     << ", with adapter stack:" << adapterStackToString()                                    \
                     << ", string tree:" << getStringTree() << ", tokens:" << getTokens());                  \
        throw parser::adapter_execution_error("Error parsing query, near \"" + queryString + '"');           \
    }

namespace lsst::qserv::ccontrol {

ParseListener::VecPairStr ParseListener::getTokenPairs(antlr4::CommonTokenStream& tokens,
                                                       QSMySqlLexer const& lexer) {
    VecPairStr ret;
    for (auto const& t : tokens.getTokens()) {
        std::string name = lexer.getVocabulary().getSymbolicName(t->getType());
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
IGNORED(DdlStatement)
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
UNHANDLED(TransactionStatement)
UNHANDLED(ReplicationStatement)
UNHANDLED(PreparedStatement)
UNHANDLED(CompoundStatement)
ENTER_EXIT_PARENT(AdministrationStatement)
UNHANDLED(UtilityStatement)
UNHANDLED(CreateDatabase)
UNHANDLED(CreateEvent)
UNHANDLED(CreateIndex)
UNHANDLED(CreateLogfileGroup)
UNHANDLED(CreateProcedure)
UNHANDLED(CreateFunction)
UNHANDLED(CreateServer)
UNHANDLED(CopyCreateTable)
UNHANDLED(QueryCreateTable)
UNHANDLED(ColumnCreateTable)
UNHANDLED(CreateTablespaceInnodb)
UNHANDLED(CreateTablespaceNdb)
UNHANDLED(CreateTrigger)
UNHANDLED(CreateView)
UNHANDLED(CreateDatabaseOption)
UNHANDLED(OwnerStatement)
UNHANDLED(PreciseSchedule)
UNHANDLED(IntervalSchedule)
UNHANDLED(TimestampValue)
UNHANDLED(IntervalExpr)
UNHANDLED(IntervalType)
UNHANDLED(EnableType)
UNHANDLED(IndexType)
UNHANDLED(IndexOption)
UNHANDLED(ProcedureParameter)
UNHANDLED(FunctionParameter)
UNHANDLED(RoutineComment)
UNHANDLED(RoutineLanguage)
UNHANDLED(RoutineBehavior)
UNHANDLED(RoutineData)
UNHANDLED(RoutineSecurity)
UNHANDLED(ServerOption)
UNHANDLED(CreateDefinitions)
UNHANDLED(ColumnDeclaration)
UNHANDLED(ConstraintDeclaration)
UNHANDLED(IndexDeclaration)
UNHANDLED(ColumnDefinition)
UNHANDLED(NullColumnConstraint)
UNHANDLED(DefaultColumnConstraint)
UNHANDLED(AutoIncrementColumnConstraint)
UNHANDLED(PrimaryKeyColumnConstraint)
UNHANDLED(UniqueKeyColumnConstraint)
UNHANDLED(CommentColumnConstraint)
UNHANDLED(FormatColumnConstraint)
UNHANDLED(StorageColumnConstraint)
UNHANDLED(ReferenceColumnConstraint)
UNHANDLED(PrimaryKeyTableConstraint)
UNHANDLED(UniqueKeyTableConstraint)
UNHANDLED(ForeignKeyTableConstraint)
UNHANDLED(CheckTableConstraint)
UNHANDLED(ReferenceDefinition)
UNHANDLED(ReferenceAction)
UNHANDLED(ReferenceControlType)
UNHANDLED(SimpleIndexDeclaration)
UNHANDLED(SpecialIndexDeclaration)
UNHANDLED(TableOptionEngine)
UNHANDLED(TableOptionAutoIncrement)
UNHANDLED(TableOptionAverage)
UNHANDLED(TableOptionCharset)
UNHANDLED(TableOptionChecksum)
UNHANDLED(TableOptionCollate)
UNHANDLED(TableOptionComment)
UNHANDLED(TableOptionCompression)
UNHANDLED(TableOptionConnection)
UNHANDLED(TableOptionDataDirectory)
UNHANDLED(TableOptionDelay)
UNHANDLED(TableOptionEncryption)
UNHANDLED(TableOptionIndexDirectory)
UNHANDLED(TableOptionInsertMethod)
UNHANDLED(TableOptionKeyBlockSize)
UNHANDLED(TableOptionMaxRows)
UNHANDLED(TableOptionMinRows)
UNHANDLED(TableOptionPackKeys)
UNHANDLED(TableOptionPassword)
UNHANDLED(TableOptionRowFormat)
UNHANDLED(TableOptionRecalculation)
UNHANDLED(TableOptionPersistent)
UNHANDLED(TableOptionSamplePage)
UNHANDLED(TableOptionTablespace)
UNHANDLED(TableOptionUnion)
UNHANDLED(TablespaceStorage)
UNHANDLED(PartitionDefinitions)
UNHANDLED(PartitionFunctionHash)
UNHANDLED(PartitionFunctionKey)
UNHANDLED(PartitionFunctionRange)
UNHANDLED(PartitionFunctionList)
UNHANDLED(SubPartitionFunctionHash)
UNHANDLED(SubPartitionFunctionKey)
UNHANDLED(PartitionComparision)
UNHANDLED(PartitionListAtom)
UNHANDLED(PartitionListVector)
UNHANDLED(PartitionSimple)
UNHANDLED(PartitionDefinerAtom)
UNHANDLED(PartitionDefinerVector)
UNHANDLED(SubpartitionDefinition)
UNHANDLED(PartitionOptionEngine)
UNHANDLED(PartitionOptionComment)
UNHANDLED(PartitionOptionDataDirectory)
UNHANDLED(PartitionOptionIndexDirectory)
UNHANDLED(PartitionOptionMaxRows)
UNHANDLED(PartitionOptionMinRows)
UNHANDLED(PartitionOptionTablespace)
UNHANDLED(PartitionOptionNodeGroup)
UNHANDLED(AlterSimpleDatabase)
UNHANDLED(AlterUpgradeName)
UNHANDLED(AlterEvent)
UNHANDLED(AlterFunction)
UNHANDLED(AlterInstance)
UNHANDLED(AlterLogfileGroup)
UNHANDLED(AlterProcedure)
UNHANDLED(AlterServer)
UNHANDLED(AlterTable)
UNHANDLED(AlterTablespace)
UNHANDLED(AlterView)
UNHANDLED(AlterByTableOption)
UNHANDLED(AlterByAddColumn)
UNHANDLED(AlterByAddColumns)
UNHANDLED(AlterByAddIndex)
UNHANDLED(AlterByAddPrimaryKey)
UNHANDLED(AlterByAddUniqueKey)
UNHANDLED(AlterByAddSpecialIndex)
UNHANDLED(AlterByAddForeignKey)
UNHANDLED(AlterBySetAlgorithm)
UNHANDLED(AlterByChangeDefault)
UNHANDLED(AlterByChangeColumn)
UNHANDLED(AlterByLock)
UNHANDLED(AlterByModifyColumn)
UNHANDLED(AlterByDropColumn)
UNHANDLED(AlterByDropPrimaryKey)
UNHANDLED(AlterByDropIndex)
UNHANDLED(AlterByDropForeignKey)
UNHANDLED(AlterByDisableKeys)
UNHANDLED(AlterByEnableKeys)
UNHANDLED(AlterByRename)
UNHANDLED(AlterByOrder)
UNHANDLED(AlterByConvertCharset)
UNHANDLED(AlterByDefaultCharset)
UNHANDLED(AlterByDiscardTablespace)
UNHANDLED(AlterByImportTablespace)
UNHANDLED(AlterByForce)
UNHANDLED(AlterByValidate)
UNHANDLED(AlterByAddPartition)
UNHANDLED(AlterByDropPartition)
UNHANDLED(AlterByDiscardPartition)
UNHANDLED(AlterByImportPartition)
UNHANDLED(AlterByTruncatePartition)
UNHANDLED(AlterByCoalescePartition)
UNHANDLED(AlterByReorganizePartition)
UNHANDLED(AlterByExchangePartition)
UNHANDLED(AlterByAnalyzePartitiion)
UNHANDLED(AlterByCheckPartition)
UNHANDLED(AlterByOptimizePartition)
UNHANDLED(AlterByRebuildPartition)
UNHANDLED(AlterByRepairPartition)
UNHANDLED(AlterByRemovePartitioning)
UNHANDLED(AlterByUpgradePartitioning)
UNHANDLED(DropDatabase)
UNHANDLED(DropEvent)
UNHANDLED(DropIndex)
UNHANDLED(DropLogfileGroup)
UNHANDLED(DropProcedure)
UNHANDLED(DropFunction)
UNHANDLED(DropServer)
UNHANDLED(DropTable)
UNHANDLED(DropTablespace)
UNHANDLED(DropTrigger)
UNHANDLED(DropView)
UNHANDLED(RenameTable)
UNHANDLED(RenameTableClause)
UNHANDLED(TruncateTable)
ENTER_EXIT_PARENT(CallStatement)
UNHANDLED(DeleteStatement)
UNHANDLED(DoStatement)
UNHANDLED(HandlerStatement)
UNHANDLED(InsertStatement)
UNHANDLED(LoadDataStatement)
UNHANDLED(LoadXmlStatement)
UNHANDLED(ReplaceStatement)
UNHANDLED(ParenthesisSelect)
UNHANDLED(UnionSelect)
UNHANDLED(UnionParenthesisSelect)
UNHANDLED(UpdateStatement)
UNHANDLED(InsertStatementValue)
UNHANDLED(UpdatedElement)
UNHANDLED(AssignmentField)
UNHANDLED(LockClause)
UNHANDLED(SingleDeleteStatement)
UNHANDLED(MultipleDeleteStatement)
UNHANDLED(HandlerOpenStatement)
UNHANDLED(HandlerReadIndexStatement)
UNHANDLED(HandlerReadStatement)
UNHANDLED(HandlerCloseStatement)
UNHANDLED(SingleUpdateStatement)
UNHANDLED(MultipleUpdateStatement)
ENTER_EXIT_PARENT(OrderByClause)
ENTER_EXIT_PARENT(OrderByExpression)
UNHANDLED(TableSourceNested)
UNHANDLED(SubqueryTableItem)
UNHANDLED(TableSourcesItem)
UNHANDLED(IndexHint)
UNHANDLED(IndexHintType)
ENTER_EXIT_PARENT(InnerJoin)
UNHANDLED(StraightJoin)
UNHANDLED(OuterJoin)
ENTER_EXIT_PARENT(NaturalJoin)
UNHANDLED(QueryExpression)
UNHANDLED(QueryExpressionNointo)
UNHANDLED(QuerySpecificationNointo)
UNHANDLED(UnionParenthesis)
UNHANDLED(UnionStatement)
ENTER_EXIT_PARENT(SelectSpec)
ENTER_EXIT_PARENT(SelectStarElement)
ENTER_EXIT_PARENT(SelectFunctionElement)
ENTER_EXIT_PARENT(SelectExpressionElement)
UNHANDLED(SelectIntoVariables)
UNHANDLED(SelectIntoDumpFile)
UNHANDLED(SelectIntoTextFile)
UNHANDLED(SelectFieldsInto)
UNHANDLED(SelectLinesInto)
ENTER_EXIT_PARENT(GroupByItem)
ENTER_EXIT_PARENT(LimitClause)
UNHANDLED(StartTransaction)
UNHANDLED(BeginWork)
UNHANDLED(CommitWork)
UNHANDLED(RollbackWork)
UNHANDLED(SavepointStatement)
UNHANDLED(RollbackStatement)
UNHANDLED(ReleaseStatement)
UNHANDLED(LockTables)
UNHANDLED(UnlockTables)
UNHANDLED(SetAutocommitStatement)
UNHANDLED(SetTransactionStatement)
UNHANDLED(TransactionMode)
UNHANDLED(LockTableElement)
UNHANDLED(LockAction)
UNHANDLED(TransactionOption)
UNHANDLED(TransactionLevel)
UNHANDLED(ChangeMaster)
UNHANDLED(ChangeReplicationFilter)
UNHANDLED(PurgeBinaryLogs)
UNHANDLED(ResetMaster)
UNHANDLED(ResetSlave)
UNHANDLED(StartSlave)
UNHANDLED(StopSlave)
UNHANDLED(StartGroupReplication)
UNHANDLED(StopGroupReplication)
UNHANDLED(MasterStringOption)
UNHANDLED(MasterDecimalOption)
UNHANDLED(MasterBoolOption)
UNHANDLED(MasterRealOption)
UNHANDLED(MasterUidListOption)
UNHANDLED(StringMasterOption)
UNHANDLED(DecimalMasterOption)
UNHANDLED(BoolMasterOption)
UNHANDLED(ChannelOption)
UNHANDLED(DoDbReplication)
UNHANDLED(IgnoreDbReplication)
UNHANDLED(DoTableReplication)
UNHANDLED(IgnoreTableReplication)
UNHANDLED(WildDoTableReplication)
UNHANDLED(WildIgnoreTableReplication)
UNHANDLED(RewriteDbReplication)
UNHANDLED(TablePair)
UNHANDLED(ThreadType)
UNHANDLED(GtidsUntilOption)
UNHANDLED(MasterLogUntilOption)
UNHANDLED(RelayLogUntilOption)
UNHANDLED(SqlGapsUntilOption)
UNHANDLED(UserConnectionOption)
UNHANDLED(PasswordConnectionOption)
UNHANDLED(DefaultAuthConnectionOption)
UNHANDLED(PluginDirConnectionOption)
UNHANDLED(GtuidSet)
UNHANDLED(XaStartTransaction)
UNHANDLED(XaEndTransaction)
UNHANDLED(XaPrepareStatement)
UNHANDLED(XaCommitWork)
UNHANDLED(XaRollbackWork)
UNHANDLED(XaRecoverWork)
UNHANDLED(PrepareStatement)
UNHANDLED(ExecuteStatement)
UNHANDLED(DeallocatePrepare)
UNHANDLED(RoutineBody)
UNHANDLED(BlockStatement)
UNHANDLED(CaseStatement)
UNHANDLED(IfStatement)
UNHANDLED(IterateStatement)
UNHANDLED(LeaveStatement)
UNHANDLED(LoopStatement)
UNHANDLED(RepeatStatement)
UNHANDLED(ReturnStatement)
UNHANDLED(WhileStatement)
UNHANDLED(CloseCursor)
UNHANDLED(FetchCursor)
UNHANDLED(OpenCursor)
UNHANDLED(DeclareVariable)
UNHANDLED(DeclareCondition)
UNHANDLED(DeclareCursor)
UNHANDLED(DeclareHandler)
UNHANDLED(HandlerConditionCode)
UNHANDLED(HandlerConditionState)
UNHANDLED(HandlerConditionName)
UNHANDLED(HandlerConditionWarning)
UNHANDLED(HandlerConditionNotfound)
UNHANDLED(HandlerConditionException)
UNHANDLED(ProcedureSqlStatement)
UNHANDLED(CaseAlternative)
UNHANDLED(ElifAlternative)
UNHANDLED(AlterUserMysqlV56)
UNHANDLED(AlterUserMysqlV57)
UNHANDLED(CreateUserMysqlV56)
UNHANDLED(CreateUserMysqlV57)
UNHANDLED(DropUser)
UNHANDLED(GrantStatement)
UNHANDLED(GrantProxy)
UNHANDLED(RenameUser)
UNHANDLED(DetailRevoke)
UNHANDLED(ShortRevoke)
UNHANDLED(RevokeProxy)
UNHANDLED(SetPasswordStatement)
UNHANDLED(UserSpecification)
UNHANDLED(PasswordAuthOption)
UNHANDLED(StringAuthOption)
UNHANDLED(HashAuthOption)
UNHANDLED(SimpleAuthOption)
UNHANDLED(TlsOption)
UNHANDLED(UserResourceOption)
UNHANDLED(UserPasswordOption)
UNHANDLED(UserLockOption)
UNHANDLED(PrivelegeClause)
UNHANDLED(Privilege)
UNHANDLED(CurrentSchemaPriviLevel)
UNHANDLED(GlobalPrivLevel)
UNHANDLED(DefiniteSchemaPrivLevel)
UNHANDLED(DefiniteFullTablePrivLevel)
UNHANDLED(DefiniteTablePrivLevel)
UNHANDLED(RenameUserClause)
UNHANDLED(AnalyzeTable)
UNHANDLED(CheckTable)
UNHANDLED(ChecksumTable)
UNHANDLED(OptimizeTable)
UNHANDLED(RepairTable)
UNHANDLED(CheckTableOption)
UNHANDLED(CreateUdfunction)
UNHANDLED(InstallPlugin)
UNHANDLED(UninstallPlugin)
ENTER_EXIT_PARENT(SetVariable)
UNHANDLED(SetCharset)
UNHANDLED(SetNames)
UNHANDLED(SetPassword)
UNHANDLED(SetTransaction)
UNHANDLED(SetAutocommit)
UNHANDLED(ShowMasterLogs)
UNHANDLED(ShowLogEvents)
UNHANDLED(ShowObjectFilter)
UNHANDLED(ShowColumns)
UNHANDLED(ShowCreateDb)
UNHANDLED(ShowCreateFullIdObject)
UNHANDLED(ShowCreateUser)
UNHANDLED(ShowEngine)
UNHANDLED(ShowGlobalInfo)
UNHANDLED(ShowErrors)
UNHANDLED(ShowCountErrors)
UNHANDLED(ShowSchemaFilter)
UNHANDLED(ShowRoutine)
UNHANDLED(ShowGrants)
UNHANDLED(ShowIndexes)
UNHANDLED(ShowOpenTables)
UNHANDLED(ShowProfile)
UNHANDLED(ShowSlaveStatus)
ENTER_EXIT_PARENT(VariableClause)
UNHANDLED(ShowCommonEntity)
UNHANDLED(ShowFilter)
UNHANDLED(ShowGlobalInfoClause)
UNHANDLED(ShowSchemaEntity)
UNHANDLED(ShowProfileType)
UNHANDLED(BinlogStatement)
UNHANDLED(CacheIndexStatement)
UNHANDLED(FlushStatement)
UNHANDLED(KillStatement)
UNHANDLED(LoadIndexIntoCache)
UNHANDLED(ResetStatement)
UNHANDLED(ShutdownStatement)
UNHANDLED(TableIndexes)
UNHANDLED(SimpleFlushOption)
UNHANDLED(ChannelFlushOption)
UNHANDLED(TableFlushOption)
UNHANDLED(FlushTableOption)
UNHANDLED(LoadedTableIndexes)
UNHANDLED(SimpleDescribeStatement)
UNHANDLED(FullDescribeStatement)
UNHANDLED(HelpStatement)
UNHANDLED(UseStatement)
UNHANDLED(DescribeStatements)
UNHANDLED(DescribeConnection)
UNHANDLED(IndexColumnName)
UNHANDLED(UserName)
UNHANDLED(MysqlVariable)
UNHANDLED(CharsetName)
UNHANDLED(CollationName)
UNHANDLED(EngineName)
UNHANDLED(UuidSet)
UNHANDLED(Xid)
UNHANDLED(XuidStringId)
UNHANDLED(AuthPlugin)
ENTER_EXIT_PARENT(SimpleId)
ENTER_EXIT_PARENT(DottedId)
UNHANDLED(FileSizeLiteral)
UNHANDLED(BooleanLiteral)
UNHANDLED(HexadecimalLiteral)
ENTER_EXIT_PARENT(NullNotnull)
ENTER_EXIT_PARENT(Constant)
UNHANDLED(StringDataType)
UNHANDLED(DimensionDataType)
UNHANDLED(SimpleDataType)
UNHANDLED(CollectionDataType)
UNHANDLED(SpatialDataType)
UNHANDLED(ConvertedDataType)
UNHANDLED(LengthOneDimension)
UNHANDLED(LengthTwoDimension)
UNHANDLED(LengthTwoOptionalDimension)
ENTER_EXIT_PARENT(UidList)
UNHANDLED(Tables)
UNHANDLED(IndexColumnNames)
ENTER_EXIT_PARENT(Expressions)
UNHANDLED(ExpressionsWithDefaults)
ENTER_EXIT_PARENT(Constants)
UNHANDLED(SimpleStrings)
UNHANDLED(UserVariables)
UNHANDLED(DefaultValue)
UNHANDLED(ExpressionOrDefault)
UNHANDLED(IfExists)
UNHANDLED(IfNotExists)
UNHANDLED(SpecificFunctionCall)
ENTER_EXIT_PARENT(AggregateFunctionCall)
ENTER_EXIT_PARENT(ScalarFunctionCall)
ENTER_EXIT_PARENT(UdfFunctionCall)
UNHANDLED(PasswordFunctionCall)
UNHANDLED(SimpleFunctionCall)
UNHANDLED(DataTypeFunctionCall)
UNHANDLED(ValuesFunctionCall)
UNHANDLED(CaseFunctionCall)
UNHANDLED(CharFunctionCall)
UNHANDLED(PositionFunctionCall)
UNHANDLED(SubstrFunctionCall)
UNHANDLED(TrimFunctionCall)
UNHANDLED(WeightFunctionCall)
UNHANDLED(ExtractFunctionCall)
UNHANDLED(GetFormatFunctionCall)
UNHANDLED(CaseFuncAlternative)
UNHANDLED(LevelWeightList)
UNHANDLED(LevelWeightRange)
UNHANDLED(LevelInWeightListElement)
ENTER_EXIT_PARENT(AggregateWindowedFunction)
ENTER_EXIT_PARENT(ScalarFunctionName)
UNHANDLED(PasswordFunctionClause)
ENTER_EXIT_PARENT(FunctionArgs)
ENTER_EXIT_PARENT(FunctionArg)
UNHANDLED(IsExpression)
ENTER_EXIT_PARENT(NotExpression)
IGNORED(QservFunctionSpecExpression)
ENTER_EXIT_PARENT(LogicalExpression)
UNHANDLED(SoundsLikePredicate)
ENTER_EXIT_PARENT(InPredicate)
UNHANDLED(SubqueryComparasionPredicate)
ENTER_EXIT_PARENT(BetweenPredicate)
ENTER_EXIT_PARENT(IsNullPredicate)
ENTER_EXIT_PARENT(LikePredicate)
UNHANDLED(RegexpPredicate)
UNHANDLED(UnaryExpressionAtom)
UNHANDLED(CollateExpressionAtom)
UNHANDLED(SubqueryExpessionAtom)
UNHANDLED(MysqlVariableExpressionAtom)
ENTER_EXIT_PARENT(NestedExpressionAtom)
UNHANDLED(NestedRowExpressionAtom)
ENTER_EXIT_PARENT(MathExpressionAtom)
UNHANDLED(IntervalExpressionAtom)
UNHANDLED(ExistsExpessionAtom)
ENTER_EXIT_PARENT(FunctionCallExpressionAtom)
UNHANDLED(BinaryExpressionAtom)
ENTER_EXIT_PARENT(BitExpressionAtom)
UNHANDLED(UnaryOperator)
ENTER_EXIT_PARENT(LogicalOperator)
ENTER_EXIT_PARENT(BitOperator)
ENTER_EXIT_PARENT(MathOperator)
UNHANDLED(CharsetNameBase)
UNHANDLED(TransactionLevelBase)
UNHANDLED(PrivilegesBase)
UNHANDLED(IntervalTypeBase)
UNHANDLED(DataTypeBase)
IGNORED_WARN(KeywordsCanBeId, "Keyword reused as ID")  // todo emit a warning?
ENTER_EXIT_PARENT(FunctionNameBase)

}  // namespace lsst::qserv::ccontrol
