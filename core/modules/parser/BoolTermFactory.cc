// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
  * @brief BoolTermFactory is a factory class for BoolTerm objects that get
  * placed (typically) in WhereClause objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/BoolTermFactory.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/PredicateFactory.h"
#include "parser/parseTreeUtil.h"
#include "parser/ParseException.h"
#include "parser/SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes
#include "parser/ValueExprFactory.h"
#include "query/Predicate.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.BoolTermFactory");
}


namespace lsst {
namespace qserv {
namespace parser {

////////////////////////////////////////////////////////////////////////
// BoolTermFactory helper
////////////////////////////////////////////////////////////////////////
/// Functor returning true if a node matches a string
class matchStr {
public:
    matchStr(std::string const& s) : _s(s) {}
    bool operator()(antlr::RefAST a) {
        return tokenText(a) == _s;
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
    /// Print tagged tokens to a stream
    struct tagPrint {
        tagPrint(std::ostream& os_, std::string const& tag_)
            : os(os_), tag(tag_) {}
        void operator()(antlr::RefAST a) {
            os << tag << ": " << tokenText(a) << "\n";
        }
        std::ostream& os;
        std::string tag;
    };
    // Print tokens with spacing.
    struct spacePrint {
        spacePrint(std::ostream& os_) : os(os_), count(0) {}
        void operator()(antlr::RefAST a) {
            if(++count > 1) os << " ";
            os << tokenText(a);
        }
        std::ostream& os;
        int count;
    };


////////////////////////////////////////////////////////////////////////
// BoolTermFactory::bfImport
////////////////////////////////////////////////////////////////////////
/// Construct a ValueExpr (or PassTerm) term and import as appropriate
void BoolTermFactory::bfImport::operator()(antlr::RefAST a) {
    PredicateFactory _pf(*_bf._vFactory); // placeholder
    int aType = a->getType(); // for gdb
    switch(aType) {
    case SqlSQL2TokenTypes::VALUE_EXP:
        throw std::logic_error("Unexpected VALUE_EXP in parse tree");
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
    case SqlSQL2TokenTypes::LIKE_PREDICATE:
        _bfr._terms.push_back(_pf.newLikePredicate(a));
        break;
    case SqlSQL2TokenTypes::NULL_PREDICATE:
        _bfr._terms.push_back(_pf.newNullPredicate(a));
        break;
    case SqlSQL2TokenTypes::QUANTIFIED_COMP_PREDICATE:
        throw std::logic_error("QUANTIFIED_COMP_PREDICATE unsupported.");
        break;
    case SqlSQL2TokenTypes::MATCH_PREDICATE:
        throw std::logic_error("MATCH_PREDICATE unsupported.");
        break;
    case SqlSQL2TokenTypes::OVERLAPS_PREDICATE:
        throw std::logic_error("OVERLAPS_PREDICATE unsupported.");
        break;

    case SqlSQL2TokenTypes::AND_OP:
    case SqlSQL2TokenTypes::OR_OP:
        _bfr._terms.push_back(_bf.newBoolTermFactor(a));
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
BoolTermFactory::BoolTermFactory(std::shared_ptr<ValueExprFactory> vf)
    : _vFactory(vf) {
}
/// Construct a new BoolTerm from a node (delegates according to type)
query::BoolTerm::Ptr
BoolTermFactory::newBoolTerm(antlr::RefAST a) {
    query::BoolTerm::Ptr b;
    antlr::RefAST child = a->getFirstChild();
    //std::cout << "New bool term token: " << a->getType() << "\n";
    switch(a->getType()) {
    case SqlSQL2TokenTypes::OR_OP: b = newOrTerm(child); break;
    case SqlSQL2TokenTypes::AND_OP: b = newAndTerm(child); break;
    case SqlSQL2TokenTypes::BOOLEAN_FACTOR: b = newBoolFactor(child); break;
    case SqlSQL2TokenTypes::VALUE_EXP:
        throw ParseException("Unexpected VALUE_EXP, expected BOOLTERM", a);
        b = newUnknown(a); break; // Value expr not expected here.
    default:
        throw ParseException("Expected BOOLTERM, got unknown token", a);
        b = newUnknown(a); break;
    }
    return b;
}

/// Construct a new OrTerm from a node
query::OrTerm::Ptr
BoolTermFactory::newOrTerm(antlr::RefAST a) {
    query::OrTerm::Ptr p = std::make_shared<query::OrTerm>();
    multiImport<query::OrTerm> oi(*this, *p);
    matchType matchOr(SqlSQL2TokenTypes::SQL2RW_or);
    applyExcept<multiImport<query::OrTerm>,matchType> ae(oi, matchOr);
    forEachSibs(a, ae);
    return p;
}
/// Construct a new AndTerm from a node
query::AndTerm::Ptr
BoolTermFactory::newAndTerm(antlr::RefAST a) {
    query::AndTerm::Ptr p = std::make_shared<query::AndTerm>();
    multiImport<query::AndTerm> ai(*this, *p);
    matchType matchAnd(SqlSQL2TokenTypes::SQL2RW_and);
    applyExcept<multiImport<query::AndTerm>,matchType> ae(ai, matchAnd);
    forEachSibs(a, ae);
    return p;
}
/// Construct a new BoolFactor
query::BoolFactor::Ptr
BoolTermFactory::newBoolFactor(antlr::RefAST a) {
#if 0
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        std::stringstream ss;
        spacePrint sp(ss);
        forEachSibs(a, sp);
        LOGS(_log, LOG_LVL_DEBUG, "bool factor: " << ss.str());
    }
#endif
    query::BoolFactor::Ptr bf = std::make_shared<query::BoolFactor>();
    bfImport bfi(*this, *bf);
    forEachSibs(a, bfi);
    return bf;
}
/// Construct an UnknownTerm(BoolTerm)
query::UnknownTerm::Ptr
BoolTermFactory::newUnknown(antlr::RefAST a) {
    LOGS(_log, LOG_LVL_DEBUG, "unknown term: " << walkTreeString(a));
    query::UnknownTerm::Ptr p = std::make_shared<query::UnknownTerm>();
    return p;
}
/// Construct an PassTerm
query::PassTerm::Ptr
BoolTermFactory::newPassTerm(antlr::RefAST a) {
    query::PassTerm::Ptr p = std::make_shared<query::PassTerm>();
    p->_text = tokenText(a); // FIXME: Should this be a tree walk?
    return p;
}

/// Construct an BoolTermFactor
query::BoolTermFactor::Ptr
BoolTermFactory::newBoolTermFactor(antlr::RefAST a) {
    query::BoolTermFactor::Ptr p = std::make_shared<query::BoolTermFactor>();
    p->_term = newBoolTerm(a);
    return p;
}

}}} // namespace lsst::qserv::parser
