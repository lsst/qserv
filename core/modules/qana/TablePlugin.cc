// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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

/// \file
/// \brief TablePlugin implementation.
///
/// TablePlugin modifies the parsed query to assign an alias to all the table
/// references in the query from-list. It then rewrites all column references
/// (e.g. in the where clause) to use the appropriate aliases. This allows
/// changing a table reference in a query without editing anything except the
/// from-clause.
///
/// During the concrete query planning phase, TablePlugin determines whether
/// each query proposed for parallel (worker-side) execution is actually
/// parallelizable and how this should be done - that is, it determines whether
/// or not sub-chunking should be used and which director table(s) to use
/// overlap for. Finally, it rewrites table references to use name patterns
/// into which (sub-)chunk numbers can be substituted. This act of substitution
/// is the final step in generating the queries sent out to workers.
///
/// \author Daniel L. Wang, SLAC

// Class header
#include "qana/TablePlugin.h"

// System headers
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/QueryMapping.h"
#include "qana/RelationGraph.h"
#include "qana/TableInfoPool.h"

#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"
#include "query/typedefs.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/common.h"
#include "util/IterableFormatter.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.TablePlugin");

template <typename CLAUSE_T>
void matchValueExprs(lsst::qserv::query::QueryContext& context, CLAUSE_T & clause, bool matchIsRequired) {
    lsst::qserv::query::ValueExprPtrRefVector valueExprRefs;
    clause.findValueExprRefs(valueExprRefs);
    for (auto&& valueExprRef : valueExprRefs) {
        auto&& valueExprMatch = context.getValueExprMatch(valueExprRef.get());
        if (nullptr != valueExprMatch) {
            LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " replacing valueExpr " << *valueExprRef.get() <<
                    " in " << clause <<
                    " with " << *valueExprMatch);
            valueExprRef.get() = valueExprMatch;
        } else if (matchIsRequired) {
            std::ostringstream os;
            os << "Could not find a value expr match for " << *valueExprRef.get();
            throw std::logic_error(os.str());
        }
    }
}


void matchTableRefs(lsst::qserv::query::QueryContext& context,
                    std::vector<std::shared_ptr<lsst::qserv::query::ColumnRef>>& columnRefs,
                    bool matchIsRequired) {
    for (auto& columnRef : columnRefs) {
        auto tableRefMatchVec = context.getTableRefMatches(columnRef);
        // todo I think there are cases where it's ok to find 0 matches.
        // it also may be ok to find more than 1, and just use the first? TBD.
        if (tableRefMatchVec.size() == 0) {
            if (matchIsRequired) {
                std::ostringstream os;
                os << "Could not find a single table ref match for " << *columnRef <<
                    ", found:" << lsst::qserv::util::printable(tableRefMatchVec);
                throw std::logic_error(os.str());
            }
        } else {
            LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " replacing tableRef in " << *columnRef << " with " << *tableRefMatchVec[0]);
            columnRef->setTable(tableRefMatchVec[0]);
        }
    }
}


// Change the contents of the ValueExprs to use the TableRef objects that are stored in the context, instead
// of allowing these ValueExprs to own their own unique TableRef objects.
void matchTableRefs(lsst::qserv::query::QueryContext& context,
                    lsst::qserv::query::ValueExprPtrVector& valueExprs,
                    bool matchIsRequired) {
    for (auto&& valueExpr : valueExprs) {
        if (valueExpr->isStar()) {
            auto valueFactor = valueExpr->getFactor();
            auto tableRefMatch = context.getTableRefMatch(valueFactor->getTableStar());
            if (nullptr != tableRefMatch) {
                valueFactor->setStar(tableRefMatch);
            }
            continue;
        }
        // Otherwise, get all the contained column refs and handle them.
        std::vector<std::shared_ptr<lsst::qserv::query::ColumnRef>> columnRefs;
        valueExpr->findColumnRefs(columnRefs);
        matchTableRefs(context, columnRefs, matchIsRequired);
    }
}


template <typename CLAUSE_T>
void matchTableRefs(lsst::qserv::query::QueryContext& context, CLAUSE_T & clause, bool matchIsRequired) {
    lsst::qserv::query::ValueExprPtrVector valueExprs;
    clause.findValueExprs(valueExprs);
    matchTableRefs(context, valueExprs, matchIsRequired);
}


} // namespace

namespace lsst {
namespace qserv {
namespace qana {


////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
TablePlugin::applyLogical(query::SelectStmt& stmt,
                          query::QueryContext& context) {
    LOGS(_log, LOG_LVL_TRACE, "applyLogical begin:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
    query::FromList& fromList = stmt.getFromList();
    context.collectTopLevelTableSchema(fromList);

    // for each top-level ValueExpr in the SELECT list that does not have an alias, assign an alias that
    // matches the original user query and add that item to the selectListAlias list.
    for (auto& valueExpr : *(stmt.getSelectList().getValueExprList())) {
        if (not valueExpr->hasAlias()) {
            if (not valueExpr->isStar()) {
                valueExpr->setAlias(valueExpr->sqlFragment(false));
                context.addUsedValueExpr(valueExpr);
            }
        } else {
            valueExpr->setAliasIsUserDefined(true);
            context.addUsedValueExpr(valueExpr);
        }
    }

    query::TableRefList& fromListTableRefs = fromList.getTableRefList();

    // Add aliases to all table references in the from-list (if they don't exist already) and then patch the
    // other clauses so that they refer to the aliases.
    //
    // The purpose of this is to confine table name references to the from-list so that the later table-name
    // substitution is confined to modifying the from-list.
    //
    // Note also that this must happen after the default db context has been filled in, or alias lookups will
    // be incorrect.

    // make sure the TableRefs in the from list are all completetly populated (db AND table)
    for (auto&& tableRef : fromListTableRefs) {
        tableRef->verifyPopulated(context.defaultDb);
    }

    std::function<void(query::TableRef::Ptr)> aliasSetter = [&] (query::TableRef::Ptr tableRef) {
        if (nullptr == tableRef) {
            return;
        }
        if (not tableRef->hasAlias()) {
            tableRef->setAlias(tableRef->hasDb() ?
                                    tableRef->getDb() + "." + tableRef->getTable() :
                                    tableRef->getTable());
        }
        LOGS(_log, LOG_LVL_DEBUG, "adding used table ref:" << *tableRef);
        if (not context.addUsedTableRef(tableRef)) {
            throw std::logic_error("could not set alias for " + tableRef->sqlFragment());
        }
        // If the TableRef being added does have JoinRefs, add those to the list of used TableRefs.
        for (auto&& joinRef : tableRef->getJoins()) {
            aliasSetter(joinRef->getRight());
            if (joinRef->getSpec() != nullptr && joinRef->getSpec()->getOn() != nullptr) {
                std::vector<std::shared_ptr<query::ColumnRef>> columnRefs;
                joinRef->getSpec()->getOn()->findColumnRefs(columnRefs);
                matchTableRefs(context, columnRefs, true);
            }
        }
    };
    std::for_each(fromListTableRefs.begin(), fromListTableRefs.end(), aliasSetter);

    // update the dominant db in the context ("dominant" is not the same as the default db)
    if (fromListTableRefs.size() > 0) {
        context.dominantDb = fromListTableRefs[0]->getDb();
        _dominantDb = context.dominantDb;
    }

    matchTableRefs(context, *stmt.getSelectList().getValueExprList(), true);

    if (stmt.hasOrderBy()) {
        matchTableRefs(context, stmt.getOrderBy(), false);
        matchValueExprs(context, stmt.getOrderBy(), true);
    }
    if (stmt.hasWhereClause()) {
        matchTableRefs(context, stmt.getWhereClause(), true);
        matchValueExprs(context, stmt.getWhereClause(), false);
    }
    if (stmt.hasGroupBy()) {
        matchTableRefs(context, stmt.getGroupBy(), false);
        matchValueExprs(context, stmt.getGroupBy(), false);
    }
    if (stmt.hasHaving()) {
        matchTableRefs(context, stmt.getHaving(), false);
        matchValueExprs(context, stmt.getHaving(), false);
    }

    LOGS(_log, LOG_LVL_TRACE, "OnClauses of Join:");
    // and in the on clauses of all join specifications.
    for (auto&& tableRef : fromListTableRefs) {
        for (auto&& joinRef : tableRef->getJoins()) {
            auto&& joinSpec = joinRef->getSpec();
            if (joinSpec) {
                // A column name in a using clause should be unqualified,
                // so only patch on clauses.
                auto&& onBoolTerm = joinSpec->getOn();
                if (onBoolTerm) {
                    matchTableRefs(context, *onBoolTerm, false);
                    matchValueExprs(context, *onBoolTerm, false);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_TRACE, "applyLogical end:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p,
                           query::QueryContext& context)
{
    TableInfoPool pool(context.defaultDb, *context.css);
    if (!context.queryMapping) {
        context.queryMapping = std::make_shared<QueryMapping>();
    }

    if ((not p.stmtParallel.empty()) && p.stmtParallel.front() != nullptr) {
        p.stmtPreFlight = p.stmtParallel.front()->clone();
        LOGS(_log, LOG_LVL_TRACE, "set local worker query:" << p.stmtPreFlight->getQueryTemplate().sqlFragment());
    }

    // Process each entry in the parallel select statement set.
    typedef SelectStmtPtrVector::iterator Iter;
    SelectStmtPtrVector newList;
    for(Iter i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
        RelationGraph g(**i, pool);
        g.rewrite(newList, *context.queryMapping);
    }
    p.dominantDb = _dominantDb;
    p.stmtParallel.swap(newList);
}

}}} // namespace lsst::qserv::qana
