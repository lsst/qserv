// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "query/TestFactory.h"

// Third-party headers

// Qserv headers
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

std::shared_ptr<QueryContext>
TestFactory::newContext() {
    std::shared_ptr<QueryContext> context = std::make_shared<QueryContext>();
    context->defaultDb = "Somedb";
    context->username = "alice";
    return context;
}

std::shared_ptr<QueryContext>
TestFactory::newContext(std::shared_ptr<css::CssAccess> css) {
    std::shared_ptr<QueryContext> context = std::make_shared<QueryContext>();
    context->css = css;
    context->defaultDb = "Somedb";
    context->username = "alice";
    return context;
}

void TestFactory::addSelectField(std::shared_ptr<SelectStmt> const& stmt, StringVector const& fields) {
    std::shared_ptr<SelectList> sl = std::make_shared<SelectList>();

    typedef StringVector::const_iterator It;
    for (It i = fields.begin(), e = fields.end(); i != e; ++i) {
        std::shared_ptr<ColumnRef> cr = std::make_shared<ColumnRef>("","","foo");
        ValueFactorPtr fact(ValueFactor::newColumnRefFactor(cr));
        ValueExprPtr expr = std::make_shared<ValueExpr>();
        expr->getFactorOps().push_back(ValueExpr::FactorOp(fact));
        sl->getValueExprList()->push_back(expr);
    }

    stmt->setSelectList(sl);
}

void TestFactory::addFrom(std::shared_ptr<SelectStmt> const& stmt) {
    TableRefListPtr refp = std::make_shared<TableRefList>();
    TableRef::Ptr tr = std::make_shared<TableRef>("", "Bar", "b");
    refp->push_back(tr);
    FromList::Ptr fl = std::make_shared<FromList>(refp);
    stmt->setFromList(fl);
}

void TestFactory::addWhere(std::shared_ptr<SelectStmt> const& stmt) {
    std::shared_ptr<WhereClause> wc = std::make_shared<WhereClause>();
    CompPredicate::Ptr cp = std::make_shared<CompPredicate>();
    cp->left = std::make_shared<ValueExpr>(); // baz
    ValueFactorPtr fact = ValueFactor::newColumnRefFactor((ColumnRef::newShared("","b","baz")));
    cp->left->getFactorOps().push_back(ValueExpr::FactorOp(fact));
    cp->op = CompPredicate::lookupOp("==");
    cp->right = std::make_shared<ValueExpr>(); // 42
    fact = ValueFactor::newConstFactor("42");
    cp->right->getFactorOps().push_back(ValueExpr::FactorOp(fact));
    BoolFactor::Ptr bfactor = std::make_shared<BoolFactor>();
    bfactor->_terms.push_back(cp);
    wc->prependAndTerm(bfactor);
    stmt->setWhereClause(wc);
}

std::shared_ptr<SelectStmt>
TestFactory::newDuplSelectExprStmt() {
    // Create a "SELECT foo f FROM Bar b WHERE b.baz=42;
    std::shared_ptr<SelectStmt> stmt = std::make_shared<SelectStmt>();

    // SELECT foo, foo
    StringVector fields;
    fields.push_back("foo");
    fields.push_back("foo");
    addSelectField(stmt, fields);

    // FROM Bar b
    addFrom(stmt);

    // WHERE b.baz=42
    addWhere(stmt);

    return stmt;
}

std::shared_ptr<SelectStmt>
TestFactory::newSimpleStmt() {
    // Create a "SELECT foo FROM Bar b WHERE b.baz=42;
    std::shared_ptr<SelectStmt> stmt = std::make_shared<SelectStmt>();

    // SELECT foo
    StringVector fields;
    fields.push_back("foo");
    addSelectField(stmt, fields);

    // FROM Bar b
    addFrom(stmt);

    // WHERE b.baz=42
    addWhere(stmt);

    return stmt;
}

}}} // lsst::qserv::query
