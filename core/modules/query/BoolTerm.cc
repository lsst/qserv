// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @brief BoolTerm implementations.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "query/BoolTerm.h"

// System headers
#include <algorithm>
#include <iterator>
#include <stdexcept>

// Third-party headers
#include "boost/make_shared.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// BoolTerm section
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, BoolTerm const& bt) {
    return bt.putStream(os);
}

std::ostream& OrTerm::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& AndTerm::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& BoolFactor::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& UnknownTerm::putStream(std::ostream& os) const {
    return os << "--UNKNOWNTERM--";
}
std::ostream& PassTerm::putStream(std::ostream& os) const {
    return os << _text;
}
std::ostream& PassListTerm::putStream(std::ostream& os) const {
    std::copy(_terms.begin(), _terms.end(),
              std::ostream_iterator<std::string>(os, " "));
    return os;
}
std::ostream& BoolTermFactor::putStream(std::ostream& os) const {
    if(_term) { return _term->putStream(os); }
    return os;
}

namespace {
    template <typename Plist>
    inline void renderList(QueryTemplate& qt,
                           Plist const& lst,
                           std::string const& sep) {
        int count=0;
        typename Plist::const_iterator i;
        for(i = lst.begin(); i != lst.end(); ++i) {
            if(!sep.empty() && ++count > 1) { qt.append(sep); }
            if(!*i) { throw std::logic_error("Bad list term"); }
            (**i).renderTo(qt);
        }
    }
}

void OrTerm::renderTo(QueryTemplate& qt) const {
    renderList(qt, _terms, "OR");
}
void AndTerm::renderTo(QueryTemplate& qt) const {
    renderList(qt, _terms, "AND");
}
void BoolFactor::renderTo(QueryTemplate& qt) const {
    std::string s;
    renderList(qt, _terms, s);
}
void UnknownTerm::renderTo(QueryTemplate& qt) const {
    qt.append("unknown");
}
void PassTerm::renderTo(QueryTemplate& qt) const {
    qt.append(_text);
}
void PassListTerm::renderTo(QueryTemplate& qt) const {
    qt.append("(");
    StringList::const_iterator i;
    bool isFirst=true;
    for(i=_terms.begin(); i != _terms.end(); ++i) {
        if(!isFirst) {
            qt.append(",");
        }
        qt.append(*i);
        isFirst = false;
    }
    qt.append(")");
}
void BoolTermFactor::renderTo(QueryTemplate& qt) const {
    if(_term) { _term->renderTo(qt); }
}

boost::shared_ptr<BoolTerm> OrTerm::getReduced() {
    // Can I eliminate myself?
    if(_terms.size() == 1) {
        boost::shared_ptr<BoolTerm> reduced = _terms.front()->getReduced();
        if(reduced) { return reduced; }
        else { return _terms.front(); }
    } else { // Get reduced versions of my children.
        // FIXME: Apply reduction on each term.
        // If reduction was successful on any child, construct a new or-term.
    }
    return boost::shared_ptr<BoolTerm>();
}

boost::shared_ptr<BoolTerm> AndTerm::getReduced() {
    // Can I eliminate myself?
    if(_terms.size() == 1) {
        boost::shared_ptr<BoolTerm> reduced = _terms.front()->getReduced();
        if(reduced) { return reduced; }
        else { return _terms.front(); }
    } else { // Get reduced versions of my children.
        // FIXME: Apply reduction on each term.
        // If reduction was successful on any child, construct a new and-term.
    }
    return boost::shared_ptr<BoolTerm>();
}

bool BoolFactor::_reduceTerms(BfTerm::PtrList& newTerms,
                              BfTerm::PtrList& oldTerms) {
    typedef BfTerm::PtrList::iterator Iter;
    bool hasReduction = false;
    for(Iter i=oldTerms.begin(), e=oldTerms.end(); i != e; ++i) {
        BfTerm& term = **i;
        BoolTermFactor* btf = dynamic_cast<BoolTermFactor*>(&term);

        if(btf) {
            if(btf->_term) {
                boost::shared_ptr<BoolTerm> reduced = btf->_term->getReduced();
                if(reduced) {
                    BoolFactor* f =  dynamic_cast<BoolFactor*>(reduced.get());
                    if(f) { // factor in a term in a factor --> factor
                        newTerms.insert(newTerms.end(),
                                        f->_terms.begin(), f->_terms.end());
                        hasReduction = true;
                    } else {
                        // still a reduction in the term, replace
                        boost::shared_ptr<BoolTermFactor> newBtf;
                        newBtf = boost::make_shared<BoolTermFactor>();
                        newBtf->_term = reduced;
                        newTerms.push_back(newBtf);
                        hasReduction = true;
                    }
                } else { // The bfterm's term couldn't be reduced,
                    // so just add it.
                    newTerms.push_back(*i);
                }
            } else { // Term-less bool term factor. Ignore.
                hasReduction = true;
            }
        } else {
            // add old bfterm
            newTerms.push_back(*i);
        }
    }
    return hasReduction;
}

bool BoolFactor::_checkParen(BfTerm::PtrList& terms) {
    if(terms.size() != 3) { return false; }

    PassTerm* pt = dynamic_cast<PassTerm*>(terms.front().get());
    if(!pt || (pt->_text != "(")) { return false; }

    pt = dynamic_cast<PassTerm*>(terms.back().get());
    if(!pt || (pt->_text != ")")) { return false; }

    return true;
}

boost::shared_ptr<BoolTerm> BoolFactor::getReduced() {
    // Get reduced versions of my children.
    BfTerm::PtrList newTerms;
    bool hasReduction = false;
    hasReduction = _reduceTerms(newTerms, _terms);
    // Parentheses reduction
    if(_checkParen(newTerms)) {
        newTerms.pop_front();
        newTerms.pop_back();
        hasReduction = true;
    }
    if(hasReduction) {
        BoolFactor* bf = new BoolFactor();
        bf->_terms = newTerms;
#if 0
        QueryTemplate qt;
        bf->renderTo(qt);
        LOGF_DEBUG("reduced. %1%" % qt.generate());
#endif
        return boost::shared_ptr<BoolFactor>(bf);
    } else {
        return boost::shared_ptr<BoolTerm>();
    }
}

namespace {
    struct syntaxCopy {
        inline BoolTerm::Ptr operator()(BoolTerm::Ptr const& t) {
            return t ? t->copySyntax() : BoolTerm::Ptr();
        }
        inline BfTerm::Ptr operator()(BfTerm::Ptr const& t) {
            return t ? t->copySyntax() : BfTerm::Ptr();
        }
    };

    struct deepCopy {
        inline BoolTerm::Ptr operator()(BoolTerm::Ptr const& t) {
            return t ? t->clone() : BoolTerm::Ptr();
        }
        inline BfTerm::Ptr operator()(BfTerm::Ptr const& t) {
            return t ? t->clone() : BfTerm::Ptr();
        }
    };

    template <typename List, class Copy>
    inline void copyTerms(List& dest, List const& src) {
        std::transform(src.begin(), src.end(), std::back_inserter(dest), Copy());
    }
} // anonymous namespace

boost::shared_ptr<BoolTerm> OrTerm::clone() const {
    boost::shared_ptr<OrTerm> ot = boost::make_shared<OrTerm>();
    copyTerms<BoolTerm::PtrList, deepCopy>(ot->_terms, _terms);
    return ot;
}
boost::shared_ptr<BoolTerm> AndTerm::clone() const {
    boost::shared_ptr<AndTerm> t = boost::make_shared<AndTerm>();
    copyTerms<BoolTerm::PtrList, deepCopy>(t->_terms, _terms);
    return t;
}
boost::shared_ptr<BoolTerm> BoolFactor::clone() const {
    boost::shared_ptr<BoolFactor> t = boost::make_shared<BoolFactor>();
    copyTerms<BfTerm::PtrList, deepCopy>(t->_terms, _terms);
    return t;
}

boost::shared_ptr<BoolTerm> UnknownTerm::clone() const {
    return  boost::make_shared<UnknownTerm>(); // TODO what is unknown now?
}

BfTerm::Ptr PassListTerm::clone() const {
    PassListTerm* p = new PassListTerm;
    std::copy(_terms.begin(), _terms.end(), std::back_inserter(p->_terms));
    return BfTerm::Ptr(p);
}
BfTerm::Ptr BoolTermFactor::clone() const {
    BoolTermFactor* p = new BoolTermFactor;
    if(_term) { p->_term = _term->clone(); }
    return BfTerm::Ptr(p);
}

// copySyntax
boost::shared_ptr<BoolTerm> OrTerm::copySyntax() const {
    boost::shared_ptr<OrTerm> ot = boost::make_shared<OrTerm>();
    copyTerms<BoolTerm::PtrList, syntaxCopy>(ot->_terms, _terms);
    return ot;
}
boost::shared_ptr<BoolTerm> AndTerm::copySyntax() const {
    boost::shared_ptr<AndTerm> at = boost::make_shared<AndTerm>();
    copyTerms<BoolTerm::PtrList, syntaxCopy>(at->_terms, _terms);
    return at;
}
boost::shared_ptr<BoolTerm> BoolFactor::copySyntax() const {
    boost::shared_ptr<BoolFactor> bf = boost::make_shared<BoolFactor>();
    copyTerms<BfTerm::PtrList, syntaxCopy>(bf->_terms, _terms);
    return bf;
}

BfTerm::Ptr PassTerm::copySyntax() const {
    PassTerm* p = new PassTerm;
    p->_text = _text;
    return BfTerm::Ptr(p);
}
BfTerm::Ptr PassListTerm::copySyntax() const {
    PassListTerm* p = new PassListTerm;
    p->_terms = _terms;
    return BfTerm::Ptr(p);
}
BfTerm::Ptr BoolTermFactor::copySyntax() const {
    BoolTermFactor* p = new BoolTermFactor;
    if(_term) { p->_term = _term->copySyntax(); }
    return BfTerm::Ptr(p);
}

}}} // namespace lsst::qserv::query
