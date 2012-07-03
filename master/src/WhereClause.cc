/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// WhereClause.cc houses the implementation of a WhereClause, a
// container for the parsed elements of a SQL WHERE.
#include "lsst/qserv/master/WhereClause.h"

#include <iostream>
#include "lsst/qserv/master/QueryTemplate.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::QsRestrictor;
using lsst::qserv::master::WhereClause;

namespace { // File-scope helpers
}

namespace lsst { namespace qserv { namespace master {
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
// QsRestrictor::render
////////////////////////////////////////////////////////////////////////
void 
QsRestrictor::render::operator()(QsRestrictor::Ptr const& p) {
    if(p.get()) {
        qt.append(p->_name);
        qt.append("(");
        StringList::const_iterator i;
        int c=0;
        for(i=p->_params.begin(); i != p->_params.end(); ++i) {
            if(++c > 1) qt.append(",");
            qt.append(*i);
        }
        qt.append(")");
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
    assert(rootAnd);
    
    
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
    }
    if(setupOk) {
        setupOk = _setupBfIter();
    }
    if(!setupOk) {
        while(_posStack.size() > 0) { _posStack.pop(); }
        _wc = NULL; 
    } // Nothing is valid.
}

void WhereClause::ValueExprIter::increment() {
    while(1) {
        _incrementBfTerm(); // Advance
        if(_posStack.empty()) {
            _wc = NULL; // Clear out WhereClause ptr
            return; 
        }
        if(_checkForExpr()) return;
    }
}

qMaster::ValueExprTerm* WhereClause::ValueExprIter::_checkForExpr() {
    BfTerm::Ptr b = *_bfIter;
    ValueExprTerm* vet = dynamic_cast<ValueExprTerm*>(b.get());
    return vet;
}

qMaster::ValueExprTerm* WhereClause::ValueExprIter::_checkForExpr() const {
    BfTerm::Ptr b = *_bfIter;
    ValueExprTerm* vet = dynamic_cast<ValueExprTerm*>(b.get());
    return vet;
}

void WhereClause::ValueExprIter::_incrementBfTerm() {
    assert(_bfIter != _bfEnd);
    ++_bfIter;
    if(_bfIter == _bfEnd) {
        _incrementBterm();
        return;
    } 
}

void WhereClause::ValueExprIter::_incrementBterm() {
    assert(!_posStack.empty());
    PosTuple& tuple = _posStack.top();
    ++tuple.first; // Advance
    if(tuple.first == tuple.second) { // At the end? then pop the stack
        _posStack.pop();
        if(_posStack.empty()) { // No more to traverse?
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
    return _posStack == other._posStack;
}

qMaster::ValueExprPtr & WhereClause::ValueExprIter::dereference() const {
    static ValueExprPtr nullPtr;
    ValueExprTerm * vet = _checkForExpr();
    assert(vet);
    return vet->_expr;
}

qMaster::ValueExprPtr& WhereClause::ValueExprIter::dereference() {
    static ValueExprPtr nullPtr;
    ValueExprTerm* vet = _checkForExpr();
    assert(vet);
    return vet->_expr;
}

bool WhereClause::ValueExprIter::_findFactor() {
    assert(!_posStack.empty());
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
bool WhereClause::ValueExprIter::_setupBfIter() {
    // Return true if we successfully setup a valid _bfIter;
    assert(!_posStack.empty());
    PosTuple& tuple = _posStack.top();
    BoolTerm::Ptr tptr = *tuple.first;
    assert(tptr.get());
    BoolFactor* bf = dynamic_cast<BoolFactor*>(tptr.get());
    if(bf) {
        _bfIter = bf->_terms.begin();
        _bfEnd = bf->_terms.end();
        return true;
    } else {
        // Try recursing deeper.
        // FIXME
        return false;
    }
}

}}} // namespace lsst::qserv::master
