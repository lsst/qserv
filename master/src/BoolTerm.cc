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
  * @file BoolTerm.cc
  *
  * @brief BoolTerm and BoolTermFactory implementations.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/BoolTerm.h"
#include <stdexcept>
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/ValueExpr.h"

namespace lsst {
namespace qserv {
namespace master {
////////////////////////////////////////////////////////////////////////
// BoolTerm section
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, BoolTerm const& bt) {
    return bt.putStream(os);
}

std::ostream& OrTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& AndTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& BoolFactor::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& UnknownTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& PassTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& PassListTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& ValueExprTerm::putStream(std::ostream& os) const {
    // FIXME
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
void ValueExprTerm::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(_expr);
    if(!_expr) { throw std::invalid_argument("Null-ValueExpr for renderTo()"); }
}

void BoolFactor::findColumnRefs(ColumnRefMap::List& list) {
    BfTerm::PtrList::const_iterator i;
    for(i = _terms.begin(); i != _terms.end(); ++i) {
        (**i).findColumnRefs(list);
    }
}
void ValueExprTerm::findColumnRefs(ColumnRefMap::List& list) {
    if(_expr) { _expr->findColumnRefs(list); }
}
boost::shared_ptr<BoolTerm> OrTerm::copySyntax() {
    boost::shared_ptr<OrTerm> ot(new OrTerm());
    ot->_terms = _terms; // shallow copy for now
    return ot;
}
boost::shared_ptr<BoolTerm> AndTerm::copySyntax() {
    boost::shared_ptr<AndTerm> at(new AndTerm());
    at->_terms = _terms; // shallow copy for now
    return at;
}
}}} // lsst::qserv::master
