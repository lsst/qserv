/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file WhereClause.cc
  *
  * @brief WhereClause is a parse element construct for SQL WHERE.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/WhereClause.h"

#include <iostream>
#include <stdexcept>
#include "query/Predicate.h"
#include "query/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace query {

BoolTerm::Ptr findAndTerm(BoolTerm::Ptr tree) {
    while(1) {
        AndTerm* at = dynamic_cast<AndTerm*>(tree.get());
        if(at) {
            return tree;
        } else {
            OrTerm* ot = dynamic_cast<OrTerm*>(tree.get());
            if(ot && (ot->_terms.size() == 1)) {
                tree = ot->_terms.front();
                continue;
            } else {
                return tree;
            }
        }
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
    BoolTerm::Ptr t = findAndTerm(_tree);
    return boost::dynamic_pointer_cast<AndTerm>(t);
}

WhereClause::ValueExprIter WhereClause::vBegin() {
    return ValueExprIter(this, _tree);
}

WhereClause::ValueExprIter WhereClause::vEnd() {
    return ValueExprIter(); // end iterators == default-constructed iterators
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

boost::shared_ptr<WhereClause> WhereClause::copyDeep() const {
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
    boost::shared_ptr<BoolTerm> insertPos = findAndTerm(_tree);

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

////////////////////////////////////////////////////////////////////////
// WhereClause::ValueExprIter
////////////////////////////////////////////////////////////////////////
WhereClause::ValueExprIter::ValueExprIter(WhereClause* wc,
                                          boost::shared_ptr<BoolTerm> bPos)
    : _wc(wc) {
    // How to iterate: walk the bool term tree!
    // Starting point: BoolTerm
    // _bPos = tree
    // _bIter = _bPos->iterBegin()
    bool setupOk = bPos.get();
    if(setupOk) {
        PosTuple p(bPos->iterBegin(), bPos->iterEnd()); // Initial position
        _posStack.push(p); // Put it on the stack.
        setupOk = _findFactor();
        if(setupOk) {
            setupOk = _setupBfIter();
        }
    }
    if(!setupOk) {
        while(_posStack.size() > 0) { _posStack.pop(); }
        _wc = NULL;
    } // Nothing is valid.
}

void WhereClause::ValueExprIter::increment() {
    _incrementValueExpr(); // Advance
    if(_posStack.empty()) {
        _wc = NULL; // Clear out WhereClause ptr
        return;
    }
    if(_checkIfValid()) return;
}

bool WhereClause::ValueExprIter::_checkIfValid() const {
    return _vIter != _vEnd;
}

void WhereClause::ValueExprIter::_incrementValueExpr() {
    assert(_vIter != _vEnd);
    ++_vIter;
    if(_vIter == _vEnd) {
        _incrementBfTerm();
        return;
    }
}

void WhereClause::ValueExprIter::_incrementBfTerm() {
    if(_bfIter == _bfEnd) {
        throw std::logic_error("Already at end of iteration.");
    }
    ++_bfIter;
    if(_bfIter == _bfEnd) {
        _incrementBterm();
        return;
    } else {
        _updateValueExprIter();
        assert(_vIter != _vEnd);
    }
}

void WhereClause::ValueExprIter::_incrementBterm() {
    if(_posStack.empty()) {
        throw std::logic_error("Missing _posStack context for _incrementBterm");
    }
    PosTuple& tuple = _posStack.top();
    ++tuple.first; // Advance
    if(tuple.first == tuple.second) { // At the end? then pop the stack
        _posStack.pop();
        if(_posStack.empty()) { // No more to traverse?
            _reset(); // Set to default-constructed iterator to match end.
            return;
        } else {
            _incrementBterm();
            return;
        }
    }
    if(!_setupBfIter()) { _incrementBterm(); }
}
bool WhereClause::ValueExprIter::equal(WhereClause::ValueExprIter const& other) const {
    // Compare the posStack (only .first) and the bfIter.
    if(this->_wc != other._wc) return false;
    if(!this->_wc) return true; // Both are NULL
    return (_posStack == other._posStack)
        && (_bfIter == other._bfIter)
        && (_vIter == other._vIter);
}

ValueExprPtr & WhereClause::ValueExprIter::dereference() const {
    if(_vIter == _vEnd) {
        throw std::invalid_argument("Cannot dereference end iterator");
    }
    return *_vIter;
}

ValueExprPtr& WhereClause::ValueExprIter::dereference() {
    if(_vIter == _vEnd) {
        throw std::invalid_argument("Cannot dereference end iterator");
    }
    return *_vIter;
}

bool WhereClause::ValueExprIter::_findFactor() {
    if(_posStack.empty()) {
        throw std::logic_error("Missing state: invalid _posStack ");
    }
    while(true) {
        PosTuple& tuple = _posStack.top();
        BoolTerm::Ptr tptr = *tuple.first;
        PosTuple p(tptr->iterBegin(), tptr->iterEnd());
        if(p.first != p.second) { // Should go deeper
            _posStack.push(p); // Put it on the stack.
        } else { // Leaf BoolTerm, ready to setup BoolFactor.
            return true;
        }
    }
    return false; // Should not get here.
}

void WhereClause::ValueExprIter::_reset() {
    _wc = NULL; // NULL _wc is enough to compare as true with default iterator
}

bool WhereClause::ValueExprIter::_setupBfIter() {
    // Return true if we successfully setup a valid _bfIter;
    if(_posStack.empty()) {
        throw std::logic_error("Missing state: invalid _posStack ");
    }
    PosTuple& tuple = _posStack.top();
    BoolTerm::Ptr tptr = *tuple.first;
    if(!tptr) {
        throw std::logic_error("Invalid _posStack state.");
    }
    BoolFactor* bf = dynamic_cast<BoolFactor*>(tptr.get());
    if(bf) {
        _bfIter = bf->_terms.begin();
        _bfEnd = bf->_terms.end();
        _updateValueExprIter();

        return _vIter != _vEnd;
    } else {
        // Try recursing deeper.
        // FIXME
        return false;
    }
}
void WhereClause::ValueExprIter::_updateValueExprIter() {
    _vIter = _vEnd = ValueExprListIter();
    if(_bfIter == _bfEnd) {
        return;
    }
    BfTerm::Ptr b = *_bfIter;
    assert(b);
    Predicate* p = dynamic_cast<Predicate*>(b.get());
    if(!p) {
        return;
    }
    p->cacheValueExprList();
    _vIter = p->valueExprCacheBegin();
    _vEnd = p->valueExprCacheEnd();
}

}}} // namespace lsst::qserv::query
