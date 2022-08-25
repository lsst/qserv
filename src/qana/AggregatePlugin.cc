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
#include "qana/CheckAggregation.h"
#include "query/AggOp.h"
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

/// ConvertAgg build records for merge expressions from parallel expressions
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
            // Compute aliases as necessary to protect select list
            // elements so that result tables can be dumped and the
            // columns can be re-referenced in merge queries.
            std::string interName = origAlias;
            // If there is no user alias, the expression is unprotected
            // * cannot be protected--can't alias a set of columns
            // simple column names are already legal column names
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
        // For exprs with aggregation, we must separate out the
        // expression into pieces.
        // Split the elements of a ValueExpr into its
        // constituent ValueFactors, compute the lists in parallel, and
        // then compute the expression result from the parallel
        // results during merging.
        query::ValueExprPtr mergeExpr = std::make_shared<query::ValueExpr>();
        query::ValueExpr::FactorOpVector& mergeFactorOps = mergeExpr->getFactorOps();
        query::ValueExpr::FactorOpVector const& factorOps = e.getFactorOps();
        for (query::ValueExpr::FactorOpVector::const_iterator i = factorOps.begin(); i != factorOps.end();
             ++i) {
            query::ValueFactorPtr newFactor = i->factor->clone();
            if (newFactor->getType() != query::ValueFactor::AGGFUNC) {
                parallelList.push_back(query::ValueExpr::newSimple(newFactor));
            } else {
                query::AggRecord r;
                r.orig = newFactor;
                if (!newFactor->getFuncExpr()) {
                    throw std::logic_error("Missing FuncExpr in AggRecord");
                }
                query::AggRecord::Ptr p = aMgr.applyOp(newFactor->getFuncExpr()->getName(), *newFactor);
                if (!p) {
                    throw std::logic_error("Couldn't process AggRecord");
                }
                parallelList.insert(parallelList.end(), p->parallel.begin(), p->parallel.end());
                query::ValueExpr::FactorOp m;
                m.factor = p->merge;
                m.op = i->op;
                mergeFactorOps.push_back(m);
            }
        }
        mergeExpr->setAlias(origAlias);
        mergeList.push_back(mergeExpr);
    }

    C& parallelList;
    C& mergeList;
    query::AggOp::Mgr& aMgr;
};

////////////////////////////////////////////////////////////////////////
// AggregatePlugin implementation
////////////////////////////////////////////////////////////////////////
void AggregatePlugin::applyPhysical(QueryPlugin::Plan& plan, query::QueryContext& context) {
    // For each entry in original's SelectList, modify the SelectList for the parallel and merge versions.
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
    // Also need to operate on GROUP BY.

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
