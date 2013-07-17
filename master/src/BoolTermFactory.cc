/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file BoolTermFactory.cc
  *
  * @brief BoolTermFactory is a factory class for BoolTerm objects that get
  * placed (typically) in WhereClause objects.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/BoolTermFactory.h"
#include "lsst/qserv/master/ValueExprFactory.h" 
#include "lsst/qserv/master/Predicate.h" 
#include "lsst/qserv/master/PredicateFactory.h" 
#include "SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes

namespace qMaster=lsst::qserv::master;

namespace { // anonymous helpers

////////////////////////////////////////////////////////////////////////
// BoolTermFactory helper
////////////////////////////////////////////////////////////////////////
/// Functor returning true if a node matches a string
class matchStr {
public: 
    matchStr(std::string const& s) : _s(s) {}
    bool operator()(antlr::RefAST a) {
        return qMaster::tokenText(a) == _s;
    }
    std::string _s;
};
/// Functor returning true if a node matches a type
class matchType {
public: 
    matchType(int tokenType) : _type(tokenType) {}
    bool operator()(antlr::RefAST a) {
        return a->getType() == _type;
    }
    int _type;
};
/// Iterate over a node's siblings and apply a function.
template <class F>
void forEachSibs(antlr::RefAST a, F& f) {
    for(; a.get(); a = a->getNextSibling()) {
        f(a);
    }
}
} // anonymous namespace

namespace lsst { 
namespace qserv { 
namespace master {
////////////////////////////////////////////////////////////////////////
// BoolTermFactory::bfImport
////////////////////////////////////////////////////////////////////////
/// Construct a ValueExpr (or PassTerm) term and import as appropriate
void BoolTermFactory::bfImport::operator()(antlr::RefAST a) {
    PredicateFactory _pf(*_bf._vFactory); // placeholder
    switch(a->getType()) {
    case SqlSQL2TokenTypes::VALUE_EXP:
        _bfr._terms.push_back(_bf.newValueExprTerm(a));
        break;
    case SqlSQL2TokenTypes::COMP_PREDICATE:
        _bfr._terms.push_back(_pf.newCompPredicate(a));
        break;
    case SqlSQL2TokenTypes::BETWEEN_PREDICATE:
        _bfr._terms.push_back(_pf.newBetweenPredicate(a));
        break;
    case SqlSQL2TokenTypes::IN_PREDICATE:
        _bfr._terms.push_back(_pf.newInPredicate(a));
        break;
    default:
        _bfr._terms.push_back(_bf.newPassTerm(a));
        break;
    }
}

////////////////////////////////////////////////////////////////////////
// BoolTermFactory
////////////////////////////////////////////////////////////////////////
/// Constructor
BoolTermFactory::BoolTermFactory(boost::shared_ptr<ValueExprFactory> vf)
    : _vFactory(vf) {    
}
/// Construct a new BoolTerm from a node (delegates according to type)
BoolTerm::Ptr 
BoolTermFactory::newBoolTerm(antlr::RefAST a) {
    BoolTerm::Ptr b;
    antlr::RefAST child = a->getFirstChild();
    switch(a->getType()) {
    case SqlSQL2TokenTypes::OR_OP: b = newOrTerm(child); break;
    case SqlSQL2TokenTypes::AND_OP: b = newAndTerm(child); break;
    case SqlSQL2TokenTypes::BOOLEAN_FACTOR: b = newBoolFactor(child); break;
    case SqlSQL2TokenTypes::VALUE_EXP: 
        b = newUnknown(a); break; // Value expr not expected here.
    default:
        b = newUnknown(a); break; 
    }
    return b;
}

/// Construct a new OrTerm from a node
OrTerm::Ptr 
BoolTermFactory::newOrTerm(antlr::RefAST a) {
    qMaster::OrTerm::Ptr p(new OrTerm());
    multiImport<OrTerm> oi(*this, *p);
    matchType matchOr(SqlSQL2TokenTypes::SQL2RW_or);
    applyExcept<multiImport<OrTerm>,matchType> ae(oi, matchOr);
    forEachSibs(a, ae);
    return p;
}
/// Construct a new AndTerm from a node
AndTerm::Ptr 
BoolTermFactory::newAndTerm(antlr::RefAST a) {
    qMaster::AndTerm::Ptr p(new AndTerm());
    multiImport<AndTerm> ai(*this, *p);
    matchType matchAnd(SqlSQL2TokenTypes::SQL2RW_and);
    applyExcept<multiImport<AndTerm>,matchType> ae(ai, matchAnd);
    forEachSibs(a, ae);
    return p;
}
/// Construct a new BoolFactor 
BoolFactor::Ptr 
BoolTermFactory::newBoolFactor(antlr::RefAST a) {
#if 0
    std::cout << "bool factor:";
    spacePrint sp(std::cout);
    forEachSibs(a, sp);
    std::cout << std::endl;
#endif
    BoolFactor::Ptr bf(new BoolFactor());
    bfImport bfi(*this, *bf);
    forEachSibs(a, bfi);
    return bf;
}
/// Construct an UnknownTerm(BoolTerm) 
UnknownTerm::Ptr 
BoolTermFactory::newUnknown(antlr::RefAST a) {
    std::cout << "unknown term:" << qMaster::walkTreeString(a) << std::endl;
    return qMaster::UnknownTerm::Ptr(new qMaster::UnknownTerm());
}
/// Construct an PassTerm
PassTerm::Ptr
BoolTermFactory::newPassTerm(antlr::RefAST a) {
    qMaster::PassTerm::Ptr p(new PassTerm());
    p->_text = tokenText(a); // FIXME: Should this be a tree walk?
    return p;
}
/// Construct a new ValueExprTerm using the ValueExprFactory
ValueExprTerm::Ptr
BoolTermFactory::newValueExprTerm(antlr::RefAST a) {
    qMaster::ValueExprTerm::Ptr p(new ValueExprTerm());
    p->_expr = _vFactory->newExpr(a->getFirstChild());
    return p;
}

}}} // lsst::qserv::master
