// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#include "query/WhereClause.h"

// System headers
#include <iostream>
#include <stdexcept>

// Local headers
#include "query/Predicate.h"
#include "query/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace query {

namespace {

    BoolTerm::Ptr skipTrivialOrTerms(BoolTerm::Ptr tree) {
        OrTerm * ot = dynamic_cast<OrTerm *>(tree.get());
        while (ot && ot->_terms.size() == 1) {
            tree = ot->_terms.front();
            ot = dynamic_cast<OrTerm *>(tree.get());
        }
        return tree;
    }

}


////////////////////////////////////////////////////////////////////////
// WhereClause
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, WhereClause const& wc) {
    os << "WHERE " << wc.getGenerated();
    return os;
}
void findColumnRefs(boost::shared_ptr<BoolFactor> f, ColumnRef::List& list) {
    if(f) {
        f->findColumnRefs(list);
    }
}
void findColumnRefs(boost::shared_ptr<BoolTerm> t, ColumnRef::List& list) {
    if(!t) { return; }
    BoolTerm::PtrList::iterator i = t->iterBegin();
    BoolTerm::PtrList::iterator e = t->iterEnd();
    if(i == e) { // Leaf.
        // Bool factor?
        boost::shared_ptr<BoolFactor> bf = boost::dynamic_pointer_cast<BoolFactor>(t);
        if(bf) {
            findColumnRefs(bf, list);
        } else {
            throw std::logic_error("Unexpected non BoolFactor in BoolTerm");
        }
    } else {
        for(; i != e; ++i) {
            findColumnRefs(*i, list); // Recurse
        }
    }
}

boost::shared_ptr<ColumnRef::List const>
WhereClause::getColumnRefs() const {
    boost::shared_ptr<ColumnRef::List> list(new ColumnRef::List());

    // Idea: Walk the expression tree and add all column refs to the
    // list. We will walk in depth-first order, but the interface spec
    // doesn't require any particular order.
    findColumnRefs(_tree, *list);

    return list;
}


boost::shared_ptr<AndTerm>
WhereClause::getRootAndTerm() {
    // Walk the list to find the global AND. If an OR term is root,
    // and has multiple terms, there is no global AND which means we
    // should return NULL.
    BoolTerm::Ptr t = skipTrivialOrTerms(_tree);
    return boost::dynamic_pointer_cast<AndTerm>(t);
}

void WhereClause::findValueExprs(ValueExprList& list) {
    if (_tree) { _tree->findValueExprs(list); }
}

std::string
WhereClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.generate();
}
void WhereClause::renderTo(QueryTemplate& qt) const {
    if(_restrs.get()) {
        std::for_each(_restrs->begin(), _restrs->end(),
                      QsRestrictor::render(qt));
    }
    if(_tree.get()) {
        _tree->renderTo(qt);
    }
}

boost::shared_ptr<WhereClause> WhereClause::clone() const {
    // FIXME
    boost::shared_ptr<WhereClause> newC(new WhereClause(*this));
    // Shallow copy of expr list is okay.
    if(_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    if(_restrs.get()) {
        newC->_restrs.reset(new QsRestrictor::List(*_restrs));
    }
    // For the other fields, default-copied versions are okay.
    return newC;

}

boost::shared_ptr<WhereClause> WhereClause::copySyntax() {
    boost::shared_ptr<WhereClause> newC(new WhereClause(*this));
    // Shallow copy of expr list is okay.
    if(_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    // For the other fields, default-copied versions are okay.
    return newC;
}

void
WhereClause::prependAndTerm(boost::shared_ptr<BoolTerm> t) {
    // Walk to AndTerm and prepend new BoolTerm in front of the
    // list. If the new BoolTerm is an instance of AndTerm, prepend
    // its terms rather than the AndTerm itself.
    boost::shared_ptr<BoolTerm> insertPos = skipTrivialOrTerms(_tree);

    // FIXME: Should deal with case where AndTerm is not found.
    AndTerm* rootAnd = dynamic_cast<AndTerm*>(insertPos.get());
    if(!rootAnd) {
        boost::shared_ptr<AndTerm> a(new AndTerm());
        boost::shared_ptr<BoolTerm> oldTree(_tree);
        _tree = a;
        if(oldTree.get()) { // Only add oldTree root if non-NULL
            a->_terms.push_back(oldTree);
        }
        rootAnd = a.get();

    }
    if(!rootAnd) {
        // For now, the root AND should be there by construction. No
        // code has been written that would eliminate the root AND term.
        throw std::logic_error("Couldn't find root AND term");
    }

    AndTerm* incomingTerms = dynamic_cast<AndTerm*>(t.get());
    if(incomingTerms) {
        // Insert its elements then.
        rootAnd->_terms.insert(rootAnd->_terms.begin(),
                               incomingTerms->_terms.begin(),
                               incomingTerms->_terms.end());
    } else {
        // Just insert the term as-is.
        rootAnd->_terms.insert(rootAnd->_terms.begin(), t);
    }
}


////////////////////////////////////////////////////////////////////////
// WhereClause (private)
////////////////////////////////////////////////////////////////////////
void
WhereClause::resetRestrs() {
    _restrs.reset(new QsRestrictor::List());
}

}}} // namespace lsst::qserv::query
