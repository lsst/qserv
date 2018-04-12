// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 AURA/LSST.
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
  * @brief WhereClause is a parse element construct for SQL WHERE.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/WhereClause.h"

// System headers
#include <algorithm>
#include <iostream>
#include <memory>
#include <stdexcept>

// Third-party headers

// Qserv headers
#include "global/Bug.h"
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "util/PointerCompare.h"
#include "util/DbgPrintHelper.h"

namespace {

lsst::qserv::query::BoolTerm::Ptr
skipTrivialOrTerms(lsst::qserv::query::BoolTerm::Ptr& tree) {
    lsst::qserv::query::OrTerm * ot = dynamic_cast<lsst::qserv::query::OrTerm *>(tree.get());
    while (ot && ot->_terms.size() == 1) {
        tree = ot->_terms.front();
        ot = dynamic_cast<lsst::qserv::query::OrTerm *>(tree.get());
    }
    return tree;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// WhereClause
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, WhereClause const& wc) {
    os << "WHERE " << wc.getGenerated();
    return os;
}
void findColumnRefs(std::shared_ptr<BoolFactor> f, ColumnRef::Vector& vector) {
    if (f) {
        f->findColumnRefs(vector);
    }
}
void findColumnRefs(std::shared_ptr<BoolTerm> t, ColumnRef::Vector& vector) {
    if (!t) { return; }
    BoolTerm::PtrVector::iterator i = t->iterBegin();
    BoolTerm::PtrVector::iterator e = t->iterEnd();
    if (i == e) { // Leaf.
        // Bool factor?
        std::shared_ptr<BoolFactor> bf = std::dynamic_pointer_cast<BoolFactor>(t);
        if (bf) {
            findColumnRefs(bf, vector);
        } else {
            std::ostringstream os;
            t->putStream(os);
            throw Bug(std::string("Unexpected non BoolFactor in BoolTerm(")
                      + t->getName()
                      + "): " + os.str());
        }
    } else {
        for(; i != e; ++i) {
            findColumnRefs(*i, vector); // Recurse
        }
    }
}

std::shared_ptr<ColumnRef::Vector const>
WhereClause::getColumnRefs() const {
    std::shared_ptr<ColumnRef::Vector> vector = std::make_shared<ColumnRef::Vector>();

    // Idea: Walk the expression tree and add all column refs to the
    // list. We will walk in depth-first order, but the interface spec
    // doesn't require any particular order.
    findColumnRefs(_tree, *vector);

    return vector;
}


std::shared_ptr<AndTerm>
WhereClause::getRootAndTerm() {
    // Walk the list to find the global AND. If an OR term is root,
    // and has multiple terms, there is no global AND which means we
    // should return NULL.
    BoolTerm::Ptr t = skipTrivialOrTerms(_tree);
    return std::dynamic_pointer_cast<AndTerm>(t);
}

void WhereClause::findValueExprs(ValueExprPtrVector& vector) {
    if (_tree) { _tree->findValueExprs(vector); }
}

std::string
WhereClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.sqlFragment();
}
void WhereClause::renderTo(QueryTemplate& qt) const {
    if (_restrs != nullptr) {
        QsRestrictor::render rend(qt);
        for (auto& res : *_restrs) {
            rend.applyToQT(res);
        }
    }
    if (_tree.get()) {
        _tree->renderTo(qt);
    }
}

std::shared_ptr<WhereClause> WhereClause::clone() const {
    // FIXME
    std::shared_ptr<WhereClause> newC = std::make_shared<WhereClause>(*this);
    // Shallow copy of expr list is okay.
    if (_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    if (_restrs.get()) {
        newC->_restrs = std::make_shared<QsRestrictor::PtrVector>(*_restrs);
    }
    // For the other fields, default-copied versions are okay.
    return newC;

}

std::shared_ptr<WhereClause> WhereClause::copySyntax() {
    std::shared_ptr<WhereClause> newC = std::make_shared<WhereClause>(*this);
    // Shallow copy of expr list is okay.
    if (_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    // For the other fields, default-copied versions are okay.
    return newC;
}

void
WhereClause::prependAndTerm(std::shared_ptr<BoolTerm> t) {
    // Walk to AndTerm and prepend new BoolTerm in front of the
    // list. If the new BoolTerm is an instance of AndTerm, prepend
    // its terms rather than the AndTerm itself.
    std::shared_ptr<BoolTerm> insertPos = skipTrivialOrTerms(_tree);

    // FIXME: Should deal with case where AndTerm is not found.
    AndTerm* rootAnd = dynamic_cast<AndTerm*>(insertPos.get());
    if (!rootAnd) {
        std::shared_ptr<AndTerm> a = std::make_shared<AndTerm>();
        std::shared_ptr<BoolTerm> oldTree(_tree);
        _tree = a;
        if (oldTree.get()) { // Only add oldTree root if non-NULL
            a->_terms.push_back(oldTree);
        }
        rootAnd = a.get();

    }
    if (!rootAnd) {
        // For now, the root AND should be there by construction. No
        // code has been written that would eliminate the root AND term.
        throw std::logic_error("Couldn't find root AND term");
    }

    AndTerm* incomingTerms = dynamic_cast<AndTerm*>(t.get());
    if (incomingTerms) {
        // Insert its elements then.
        rootAnd->_terms.insert(rootAnd->_terms.begin(),
                               incomingTerms->_terms.begin(),
                               incomingTerms->_terms.end());
    } else {
        // Just insert the term as-is.
        rootAnd->_terms.insert(rootAnd->_terms.begin(), t);
    }
}


void WhereClause::dbgPrint(std::ostream& os) {
    os << "WhereClause(tree:" << util::DbgPrintPtrH<BoolTerm>(_tree);
    os << ", restrs:" << util:: DbgPrintPtrVectorPtrH<QsRestrictor>(_restrs);
    os << ")";
}

bool WhereClause::operator==(WhereClause& rhs) const {
    return (util::ptrCompare<BoolTerm>(_tree, rhs._tree) &&
            util::ptrVectorPtrCompare<QsRestrictor>(_restrs, rhs._restrs));
}


////////////////////////////////////////////////////////////////////////
// WhereClause (private)
////////////////////////////////////////////////////////////////////////
void
WhereClause::resetRestrs() {
    _restrs = std::make_shared<QsRestrictor::PtrVector>();
}


}}} // namespace lsst::qserv::query
