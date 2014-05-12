/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
/// TestFactory implementation
#include "query/TestFactory.h"

// Third-party headers
#include <boost/make_shared.hpp>

// Local headers
#include "query/FromList.h"
#include "query/Predicate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"
#include "query/QueryContext.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

namespace lsst {
namespace qserv {
namespace query {

boost::shared_ptr<QueryContext>
TestFactory::newContext() {
    boost::shared_ptr<QueryContext> context(new QueryContext());
    context->defaultDb = "Somedb";
    context->username = "alice";
    return context;
}

boost::shared_ptr<QueryContext>
TestFactory::newContext(boost::shared_ptr<css::Facade> cssFacade) {
    boost::shared_ptr<QueryContext> context(new QueryContext());
    context->cssFacade = cssFacade;
    context->defaultDb = "Somedb";
    context->username = "alice";
    return context;
}

boost::shared_ptr<SelectStmt>
TestFactory::newStmt() {
    // Create a "SELECT foo f FROM Bar b WHERE b.baz=42;
    boost::shared_ptr<SelectStmt> stmt(new SelectStmt());

    // SELECT foo f
    SelectList::Ptr sl(new SelectList());
    boost::shared_ptr<ColumnRef> cr(new ColumnRef("","","foo"));
    ValueFactorPtr fact(ValueFactor::newColumnRefFactor(cr));
    ValueExprPtr expr(new ValueExpr());
    expr->getFactorOps().push_back(ValueExpr::FactorOp(fact));
        sl->getValueExprList()->push_back(expr);
    stmt->setSelectList(sl);

    // FROM Bar b
    TableRefListPtr refp(new TableRefList());
    TableRef::Ptr tr(new TableRef("", "Bar", "b"));
    refp->push_back(tr);
    FromList::Ptr fl(new FromList(refp));
    stmt->setFromList(fl);

    // WHERE b.baz=42
    boost::shared_ptr<WhereClause> wc(new WhereClause());
    CompPredicate::Ptr cp(new CompPredicate());
    cp->left = ValueExprPtr(new ValueExpr()); // baz
    fact = ValueFactor::newColumnRefFactor((ColumnRef::newShared("","b","baz")));
    cp->left->getFactorOps().push_back(ValueExpr::FactorOp(fact));
    cp->op = CompPredicate::lookupOp("==");
    cp->right = ValueExprPtr(new ValueExpr()); // 42
    fact = ValueFactor::newConstFactor("42");
    cp->right->getFactorOps().push_back(ValueExpr::FactorOp(fact));
    BoolFactor::Ptr bfactor(new BoolFactor());
    bfactor->_terms.push_back(cp);
    wc->prependAndTerm(bfactor);
    return stmt;
}

}}} // lsst::qserv::query
