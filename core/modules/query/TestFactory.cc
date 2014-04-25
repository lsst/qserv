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
#include <boost/make_shared.hpp>
#include "query/Predicate.h"
#include "query/SelectStmt.h"
#include "query/SelectList.h"
#include "query/FromList.h"
#include "query/TableRef.h"
#include "query/QueryContext.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

namespace lsst {
namespace qserv {

namespace query {

boost::shared_ptr<master::QueryContext>
TestFactory::newContext(master::MetadataCache* mc) {
    boost::shared_ptr<master::QueryContext> context(new master::QueryContext());
    context->metadata = mc;
    context->defaultDb = "Somedb";
    context->username = "alice";
    return context;
}

boost::shared_ptr<master::SelectStmt>
TestFactory::newStmt() {
    // Create a "SELECT foo f FROM Bar b WHERE b.baz=42;
    boost::shared_ptr<master::SelectStmt> stmt(new master::SelectStmt());

    // SELECT foo f
    master::SelectList::Ptr sl(new master::SelectList());
    boost::shared_ptr<master::ColumnRef> cr(new master::ColumnRef("","","foo"));
    master::ValueFactorPtr fact(master::ValueFactor::newColumnRefFactor(cr));
    master::ValueExprPtr expr(new master::ValueExpr());
    expr->getFactorOps().push_back(master::ValueExpr::FactorOp(fact));
        sl->getValueExprList()->push_back(expr);
    stmt->setSelectList(sl);

    // FROM Bar b
    master::TableRefListPtr refp(new master::TableRefList());
    master::TableRef::Ptr tr(new master::TableRef("", "Bar", "b"));
    refp->push_back(tr);
    master::FromList::Ptr fl(new master::FromList(refp));
    stmt->setFromList(fl);

    // WHERE b.baz=42
    boost::shared_ptr<master::WhereClause> wc(new master::WhereClause());
    master::CompPredicate::Ptr cp(new master::CompPredicate());
    cp->left = master::ValueExprPtr(new master::ValueExpr()); // baz
    fact = master::ValueFactor::newColumnRefFactor((master::ColumnRef::newShared("","b","baz")));
    cp->left->getFactorOps().push_back(master::ValueExpr::FactorOp(fact));
    cp->op = master::CompPredicate::lookupOp("==");
    cp->right = master::ValueExprPtr(new master::ValueExpr()); // 42
    fact = master::ValueFactor::newConstFactor("42");
    cp->right->getFactorOps().push_back(master::ValueExpr::FactorOp(fact));
    master::BoolFactor::Ptr bfactor(new master::BoolFactor());
    bfactor->_terms.push_back(cp);
    wc->prependAndTerm(bfactor);
    return stmt;
}


}}} // lsst::qserv::query
