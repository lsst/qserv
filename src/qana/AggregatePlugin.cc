// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "qana/AggregatePlugin.h"

// System headers
#include <string>
#include <stdexcept>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/AnalysisError.h"
#include "query/AggOp.h"
#include "query/ColumnRef.h"
#include "query/FuncExpr.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/QueryTemplate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.AggregatePlugin");
}

namespace lsst::qserv::qana {

inline query::ValueExprPtr newExprFromAlias(std::string const& alias) {
    std::shared_ptr<query::ColumnRef> cr = std::make_shared<query::ColumnRef>("", "", alias);
    std::shared_ptr<query::ValueFactor> vf;
    vf = query::ValueFactor::newColumnRefFactor(cr);
    return query::ValueExpr::newSimple(vf);
}

/// ConvertAgg builds records for merge expressions from parallel expressions
/// It rewrites the select list of an aggregate query into two select lists:
///     * a *parallel* list evaluated per-chunk on the workers
///     * a *merge* list evaluated once on the czar over result rows
///
/// It is called once per SELECT item. Each call appends to both lists, with non-aggregates (such as
/// plain columns, *, scalars) passed through directly. Aggregate items are decomposed, with query::AggOp
/// splitting them into the aggregates the workers must compute, plus a merge expression that will
/// combine the partial results into their final value.
///
/// _rewriteForMerge walks the tree so aggregates nested inside scalars / function calls are found.
///
/// Example: `SELECT AVG(x) AS a FROM Object`
///           ------------------
///                  |
///                  +--> parallelList: COUNT(x) AS QS1_COUNT
///                  |                  SUM(x) AS QS2_SUM
///                  |
///                  +--> mergeList: SUM(QS2_SUM) / SUM(QS1_COUNT) AS a
///
/// NOTE: GROUP BY, ORDER BY, LIMIT and DISTINCT are not handled by ConvertAgg.
template <class C>
class ConvertAgg {
public:
    typedef typename C::value_type T;
    ConvertAgg(C& parallelList_, C& mergeList_, query::AggOp::Mgr& aMgr_)
            : parallelList(parallelList_), mergeList(mergeList_), aMgr(aMgr_) {}
    void operator()(T const& e) { _makeRecord(*e); }

private:
    void _makeRecord(query::ValueExpr const& e) {
        std::string origAlias = e.getAlias();

        if (!e.hasAggregation()) {
            // Compute aliases as necessary to ensure SELECT list elements are referenceable in intermediate
            // results so result tables can be dumped and columns can be re-referenced in merge queries.
            //
            // First, check for passthrough:
            // - If an alias was already provided, use it.
            // - `*` expands to multiple columns so it cannot be aliased
            // - bare column refs are already addressable; no need for an alias
            std::string interName = origAlias;
            if (origAlias.empty() && !e.isStar() && !e.isColumnRef()) {
                interName = aMgr.getAggName("PASS");
            }
            query::ValueExprPtr par(e.clone());
            par->setAlias(interName);
            parallelList.push_back(par);

            if (!interName.empty()) {
                query::ValueExprPtr mer = newExprFromAlias(interName);
                mergeList.push_back(mer);
                mer->setAlias(origAlias);
            } else {
                // No intermediate name (e.g., *) --> passthrough
                mergeList.push_back(e.clone());
            }
            return;
        }

        // This SELECT item contains an aggregation. Rewrite it into the expression the czar should evaluate,
        // and place per-chunk aggregates into the parallel select list.
        query::ValueExprPtr mergeExpr = _rewriteForMerge(e, e);
        mergeExpr->setAlias(origAlias);
        mergeList.push_back(mergeExpr);
    }

    /// Rewrite a ValueExpr into its merge form and update parallelList. Recurses so that aggregates in
    /// nested expressions or scalar function arguments are found.
    ///
    /// @param expr       the expression being rewritten
    /// @param selectItem the select item (used for diagnostics)
    query::ValueExprPtr _rewriteForMerge(query::ValueExpr const& expr, query::ValueExpr const& selectItem) {
        query::ValueExprPtr merged = std::make_shared<query::ValueExpr>();
        query::ValueExpr::FactorOpVector& mergeFactorOps = merged->getFactorOps();
        for (auto const& factorOp : expr.getFactorOps()) {
            mergeFactorOps.push_back(query::ValueExpr::FactorOp(
                    _rewriteFactorForMerge(*factorOp.factor, selectItem), factorOp.op));
        }
        return merged;
    }

    /// Rewrite a ValueFactor into its merge form.
    ///
    /// @param factor     the factor being rewritten
    /// @param selectItem the select item (used for diagnostics)
    query::ValueFactorPtr _rewriteFactorForMerge(query::ValueFactor const& factor,
                                                 query::ValueExpr const& selectItem) {
        if (factor.getType() == query::ValueFactor::AGGFUNC) {
            if (!factor.getFuncExpr()) {
                throw AnalysisBug("Missing FuncExpr in an aggregation ValueFactor");
            }
            for (auto const& param : factor.getFuncExpr()->params) {
                if (param->hasAggregation()) {
                    // e.g. MAX(SUM(x)). MariaDB rejects this too without a subquery.
                    throw AnalysisError(
                            "Qserv does not support an aggregate directly inside another aggregate: \"" +
                            selectItem.sqlFragment(query::QueryTemplate::NO_ALIAS) +
                            "\". Select the aggregate on its own.");
                }
            }
            query::AggRecord::Ptr record = aMgr.applyOp(factor.getFuncExpr()->getName(), factor);
            if (!record) {
                throw AnalysisBug("Couldn't process AggRecord");
            }
            parallelList.insert(parallelList.end(), record->parallel.begin(), record->parallel.end());
            return record->merge;
        }

        if (factor.hasAggregation()) {
            // Handle factors that wrap an aggregate
            switch (factor.getType()) {
                case query::ValueFactor::EXPR:
                    return query::ValueFactor::newExprFactor(_rewriteForMerge(*factor.getExpr(), selectItem));
                case query::ValueFactor::FUNCTION: {
                    auto funcExpr = factor.getFuncExpr()->clone();
                    for (auto& param : funcExpr->params) {
                        param = _rewriteForMerge(*param, selectItem);
                    }
                    return query::ValueFactor::newFuncFactor(funcExpr);
                }
                default:
                    throw AnalysisBug("Found aggregation in a factor that can't contain one: " +
                                      query::ValueFactor::getTypeString(factor.getType()));
            }
        }

        // At this point there is no aggregation in this factor. If it doesn't read any columns, the czar can
        // evaluate it directly, so it is admissible. Anything that reads a column would have to be passed
        // through under an intermediate alias, with the merge GROUP BY rewritten (NOT supported).
        query::ColumnRef::Vector columnRefs;
        factor.findColumnRefs(columnRefs);
        if (!columnRefs.empty()) {
            throw AnalysisError(
                    "Qserv does not support an aggregate combined with a column-dependent expression: \"" +
                    selectItem.sqlFragment(query::QueryTemplate::NO_ALIAS) +
                    "\". Select the aggregate on its own.");
        }
        return factor.clone();
    }

    C& parallelList;
    C& mergeList;
    query::AggOp::Mgr& aMgr;
};

////////////////////////////////////////////////////////////////////////
// AggregatePlugin implementation
////////////////////////////////////////////////////////////////////////
void AggregatePlugin::applyPhysical(QueryPlugin::Plan& plan, query::QueryContext& context) {
    // For each entry in original's SelectList, build the SelectList for the parallel and merge versions.
    // Set hasMerge to true if aggregation is detected.
    auto origSelectValueExprs = plan.stmtOriginal.getSelectList().getValueExprList();
    if (nullptr == origSelectValueExprs) {
        throw std::invalid_argument("No select list in original SelectStmt");
    }

    // Make a single new parallelSelectList and a single new mergeSelectList for all the parallel statements.
    // This assumes that the select lists are the same for all statements, which is only true if this plugin
    // is executed early enough to ensure that other fragmenting activity has not yet taken place.
    query::SelectList parallelSelectList;
    auto mergeSelectList = std::make_shared<query::SelectList>();
    query::AggOp::Mgr aggOpManager;  // Eventually, this can be shared?
    ConvertAgg<query::ValueExprPtrVector> ca(*parallelSelectList.getValueExprList(),
                                             *mergeSelectList->getValueExprList(), aggOpManager);
    std::for_each(origSelectValueExprs->begin(), origSelectValueExprs->end(), ca);

    plan.stmtMerge.setSelectList(mergeSelectList);

    // update context.
    if (plan.stmtOriginal.getDistinct() || aggOpManager.hasAggregate()) {
        context.needsMerge = true;
        context.allChunksRequired = true;
    }

    // If we are merging *and* there is not a LIMIT on the query then we can remove the ORDER BY clause from
    // the select statment (by leaving it null). Otherwise we need to keep the ORDER BY clause, we will use
    // the one from the first parallel stmt (if it has an ORDER BY clause). But, we must check to see if it
    // contains any aliased colums that were removed from the select list, in which case the order by clause
    // must not use that alias.
    std::shared_ptr<query::OrderByClause> newOrderBy;
    if ((not context.needsMerge or plan.stmtOriginal.hasLimit()) && plan.stmtParallel.front()->hasOrderBy()) {
        newOrderBy = plan.stmtParallel.front()->getOrderBy().clone();
        for (auto& orderByTerm : *newOrderBy->getTerms()) {
            bool orderByIsInSelect = false;
            for (auto const& selectListValueExpr : *parallelSelectList.getValueExprList()) {
                if (*orderByTerm.getExpr() == *selectListValueExpr) {
                    // The order by value expr still exists in the select list; we can keep it as is.
                    orderByIsInSelect = true;
                    break;
                }
            }
            if (not orderByIsInSelect) {
                // The order by value expr no longer exists in the select list; it must not use any
                // predefined alias.
                orderByTerm.getExpr()->setAlias("");
            }
        }
    }

    for (auto& parallel_query : plan.stmtParallel) {
        parallel_query->setOrderBy(newOrderBy);
        parallel_query->setSelectList(parallelSelectList.clone());
    }
}

}  // namespace lsst::qserv::qana
